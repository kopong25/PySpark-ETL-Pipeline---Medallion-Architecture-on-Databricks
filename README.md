# PySpark ETL Pipeline - Medallion Architecture on Databricks

[![PySpark](https://img.shields.io/badge/PySpark-3.5+-orange.svg)](https://spark.apache.org/)
[![Databricks](https://img.shields.io/badge/Databricks-Compatible-red.svg)](https://databricks.com/)
[![Delta Lake](https://img.shields.io/badge/Delta%20Lake-Enabled-blue.svg)](https://delta.io/)
[![License](https://img.shields.io/badge/License-MIT-green.svg)](LICENSE)

A production-ready ETL pipeline implementing the Medallion Architecture (Bronze-Silver-Gold) using PySpark on Databricks. This project demonstrates best practices in data engineering including data quality checks, schema management, and business metric aggregations.

## 📋 Table of Contents

- [Overview](#overview)
- [Architecture](#architecture)
- [Features](#features)
- [Dataset](#dataset)
- [Installation](#installation)
- [Usage](#usage)
- [Pipeline Details](#pipeline-details)
- [Results](#results)
- [Technologies](#technologies)
- [Learnings](#learnings)
- [Contributing](#contributing)
- [License](#license)

## 🎯 Overview

This project implements a complete ETL (Extract, Transform, Load) pipeline that processes e-commerce sales data through three data quality layers:

- **Bronze Layer**: Raw data ingestion from CSV files
- **Silver Layer**: Data cleaning, validation, and transformation
- **Gold Layer**: Business-ready aggregations and analytics tables

The pipeline processes **112,650+ order records** from multiple source tables and creates 6 analytical tables for business intelligence and reporting.

## 🏗️ Architecture
```
📦 Bronze Layer (Raw Data)
   ├── bronze.customer (99,441 rows)
   ├── bronze.product (32,951 rows)
   ├── bronze.order (112,650 rows)
   └── bronze.orders (99,441 rows)
          ↓
🔧 Silver Layer (Cleaned & Validated)
   └── silver.sales_cleaned (112,650 rows)
          ↓
⭐ Gold Layer (Business Metrics)
   ├── gold.sales_by_category (74 categories)
   ├── gold.customer_lifetime_value (98,666 customers)
   ├── gold.monthly_sales (24 months)
   └── gold.top_customers (1,000 top customers)
```

## ✨ Features

### Data Quality & Transformation
- ✅ Automated data type conversions (string → double, timestamp)
- ✅ Duplicate record detection and removal
- ✅ Null value handling with configurable strategies
- ✅ Data validation (e.g., negative price filtering)
- ✅ Text standardization (lowercase, trimming)
- ✅ Schema evolution management

### Business Analytics
- 📊 Sales performance by product category
- 👥 Customer lifetime value analysis
- 📈 Monthly revenue trend tracking
- 🏆 Top customer segmentation
- 🗺️ Geographic sales distribution

### Technical Excellence
- 🚀 Optimized for large-scale data processing
- 💾 Delta Lake for ACID transactions
- 🔄 Incremental data refresh capability
- 📝 Comprehensive logging and monitoring
- 🧪 Data quality validation checks

## 📊 Dataset

The pipeline processes e-commerce data with the following structure:

**Source Tables:**
- **Orders**: Order details and timestamps
- **Order Items**: Product details per order
- **Customers**: Customer demographic information
- **Products**: Product catalog with categories

**Key Metrics:**
- 112,650 total orders
- 99,441 unique customers
- 32,951 products
- 74 product categories
- 24 months of transaction history

## 🚀 Installation

### Prerequisites
- Databricks workspace (Community or Enterprise Edition)
- PySpark 3.5+
- Python 3.8+

### Setup Steps

1. **Clone the repository**
```bash
git clone https://github.com/kopong25/pyspark-etl-pipeline.git
cd pyspark-etl-pipeline
```

2. **Upload to Databricks**
- Navigate to your Databricks workspace
- Import `spark_etl_pipeline.ipynb` notebook
- Create a cluster (recommended: 2-4 workers, DBR 13.3+)

3. **Configure data paths**
```python
# Update file paths in the notebook
CUSTOMER_FP = "/FileStore/tables/your-customer-data.csv"
PRODUCT_FP = "/FileStore/tables/your-product-data.csv"
ORDER_FP = "/FileStore/tables/your-order-data.csv"
ORDERS_FP = "/FileStore/tables/your-orders-data.csv"
```

4. **Run the pipeline**
- Execute cells sequentially
- Monitor progress through print statements

## 💻 Usage

### Running the Complete Pipeline
```python
# Cell 1: Main ETL Pipeline (Bronze → Silver → Gold)
# Creates databases, cleans data, generates base metrics

# Cell 2: Additional Gold Tables
# Creates monthly trends and customer segmentation

# Cell 3: Pipeline Summary
# Displays row counts and validation results
```

### Querying Results
```python
# Query Gold tables
sales_by_category = spark.table("gold.sales_by_category")
display(sales_by_category.orderBy("total_revenue", ascending=False).limit(10))

# Monthly trend analysis
monthly_sales = spark.table("gold.monthly_sales")
display(monthly_sales.orderBy("year", "month"))

# Top customers
top_customers = spark.table("gold.top_customers")
display(top_customers.limit(20))
```

## 🔧 Pipeline Details

### Bronze Layer
```python
# Raw data ingestion with metadata
customer_df = spark.read.csv(CUSTOMER_FP, header=True) \
    .withColumn("injection_date", current_timestamp())
```

### Silver Layer
```python
# Data cleaning and validation
sales_clean = (sales_joined
    .withColumn("price", col("price").cast("double"))
    .dropDuplicates()
    .dropna(subset=["order_id", "customer_id"])
    .filter(col("price") >= 0)
    .withColumn("order_status", trim(lower(col("order_status"))))
)
```

### Gold Layer
```python
# Business aggregations
sales_by_category = silver_df.groupBy("product_category_name").agg(
    count("order_id").alias("total_orders"),
    sum("price").alias("total_revenue"),
    avg("price").alias("avg_order_value")
)
```

## 📈 Results

### Pipeline Performance
- **Processing Time**: ~2-3 minutes (4-node cluster)
- **Data Quality**: 100% valid records in Gold layer
- **Deduplication**: Removed duplicate records in Silver layer
- **Schema Validation**: All type conversions successful

### Business Insights Generated
- Top product category by revenue identified
- Customer segmentation for targeted marketing
- Monthly revenue trends for forecasting
- Geographic sales distribution analysis

## 🛠️ Technologies

- **Apache Spark**: Distributed data processing
- **PySpark**: Python API for Spark
- **Databricks**: Cloud-based analytics platform
- **Delta Lake**: Storage layer with ACID transactions
- **Python 3.8+**: Core programming language

## 📚 Learnings

### Key Takeaways

1. **Layer Separation**: Clean data in Silver, aggregate in Gold
2. **Schema Management**: Use `overwriteSchema` for schema evolution
3. **Join Optimization**: Drop duplicate columns before joins
4. **Error Handling**: Implement comprehensive null and type checks
5. **Documentation**: Clear comments and progress indicators

### Challenges Solved

- ❌ **Problem**: Duplicate `injection_date` columns from multiple joins
  - ✅ **Solution**: Drop metadata columns before joining

- ❌ **Problem**: Schema mismatch errors during writes
  - ✅ **Solution**: Use `.option("overwriteSchema", "true")`

- ❌ **Problem**: Type conversion failures
  - ✅ **Solution**: Cast strings to numeric types explicitly

## 🤝 Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

1. Fork the repository
2. Create your feature branch (`git checkout -b feature/AmazingFeature`)
3. Commit your changes (`git commit -m 'Add some AmazingFeature'`)
4. Push to the branch (`git push origin feature/AmazingFeature`)
5. Open a Pull Request

## 📝 License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## 👤 Author

**Your Name**
- LinkedIn: (https://www.linkedin.com/in/k-oppong/)
- GitHub: [@kopong25](https://github.com/kopong25)
- Email: koppongsefa@gmail.com

## 🙏 Acknowledgments

- Databricks documentation and community
- Apache Spark contributors
- E-commerce dataset providers

---

⭐ If you found this project helpful, please give it a star!

📧 Questions? Feel free to reach out or open an issue.
```

---

# Additional GitHub Files

### `.gitignore`
```
# Databricks
.databricks/
*.pyc
__pycache__/

# Data files
*.csv
*.parquet
data/

# IDE
.vscode/
.idea/
*.swp

# OS
.DS_Store
Thumbs.db

# Logs
*.log
```

### `LICENSE` (MIT)
```
MIT License

Copyright (c) 2026 [Kwadwo Oppong]

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.

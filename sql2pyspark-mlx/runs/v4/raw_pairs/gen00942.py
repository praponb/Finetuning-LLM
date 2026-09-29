from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("sale_date") <= "2025-02-01")
    .groupBy(
        F.year("sale_date").alias("sale_year"),
        F.quarter("sale_date").alias("sale_quarter"),
    )
    .agg(F.count("*").alias("row_count"))
    .orderBy(F.col("sale_year").desc(), "sale_quarter")
    .limit(5)
)

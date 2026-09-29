from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("sale_date").between("2023-03-01", "2024-09-01"))
    .groupBy("product_id", F.month("sale_date").alias("sale_month"))
    .agg(
        F.min("units").alias("min_units"),
        F.max("sale_date").alias("last_sale_date"),
        F.sum("revenue").alias("total_revenue"),
    )
)

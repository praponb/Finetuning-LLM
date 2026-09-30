from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter((F.col("product_id") == 8) & (F.col("sale_date") <= "2025-07-22"))
    .agg(
        F.avg("units").alias("avg_units"),
        F.min("sale_date").alias("first_sale_date"),
        F.sum("revenue").alias("total_revenue"),
    )
    .orderBy(F.col("avg_units").desc())
)

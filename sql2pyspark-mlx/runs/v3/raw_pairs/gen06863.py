from pyspark.sql import functions as F

base = (
    df_store_sales
    .filter((F.col("product_id") == 9) | (F.col("sale_date") > "2025-06-01"))
)

summary = (
    base
    .groupBy("store_id", "product_id")
    .agg(
        F.max("sale_date").alias("latest_sale_date"),
        F.avg("units").alias("avg_units"),
    )
)

result = (
    summary
    .filter(F.col("avg_units") >= 19)
    .orderBy(F.col("avg_units").desc())
)

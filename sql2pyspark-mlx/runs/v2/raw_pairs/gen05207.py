from pyspark.sql import functions as F

base = df_store_sales.filter(F.col("revenue").between(1900, 3900) & (F.col("units") > 38))

summary = (
    base
    .groupBy("product_id", "store_id")
    .agg(
        F.round(F.avg("revenue"), 2).alias("avg_revenue"),
        F.max("sale_date").alias("latest_sale_date"),
    )
)

result = (
    summary
    .filter(F.col("avg_revenue") < 51)
    .select("product_id", "store_id", "avg_revenue", "latest_sale_date")
)

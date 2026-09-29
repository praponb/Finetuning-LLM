from pyspark.sql import functions as F

base = df_store_sales.filter(F.col("units").isNotNull())

summary = (
    base
    .groupBy("product_id", "store_id")
    .agg(F.min("revenue").alias("min_revenue"))
)

result = summary.orderBy("store_id")

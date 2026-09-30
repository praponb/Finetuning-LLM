from pyspark.sql import functions as F

base = df_store_sales.filter(F.col("product_id").isin([5, 7]))

agg = (
    base
    .groupBy(
        "store_id",
        F.when(F.col("revenue") >= 3300, "high").when(F.col("revenue") >= 1100, "mid").otherwise("low").alias("revenue_bucket"),
    )
    .agg(
        F.avg("units").alias("avg_units"),
        F.min("revenue").alias("min_revenue"),
    )
)

result = (
    agg
    .filter(F.col("min_revenue") < 3300)
    .select("store_id", "revenue_bucket", "avg_units")
)

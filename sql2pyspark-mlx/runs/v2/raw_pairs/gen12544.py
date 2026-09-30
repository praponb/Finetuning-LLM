from pyspark.sql import functions as F

src = df_store_sales.filter(F.col("store_id").isin([2, 5, 6]))

summary = (
    src
    .groupBy("product_id")
    .agg(
        F.round(F.avg("revenue"), 2).alias("avg_revenue"),
        (F.max("revenue") - F.min("revenue")).alias("revenue_range"),
    )
)

result = (
    summary
    .filter(F.col("avg_revenue") < 29)
    .orderBy(F.col("product_id").desc(), "avg_revenue")
)

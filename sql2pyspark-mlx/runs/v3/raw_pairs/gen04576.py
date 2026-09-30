from pyspark.sql import functions as F

src = (
    df_order_items
    .filter((F.col("quantity") <= 5) & F.col("quantity").between(4, 8) & F.col("order_id").isin([9, 12, 15, 31]))
)

summary = (
    src
    .groupBy(
        F.when(F.col("quantity") >= 9, "high").when(F.col("quantity") >= 3, "mid").otherwise("low").alias("quantity_bucket"),
    )
    .agg(
        F.round(F.avg("quantity"), 2).alias("avg_quantity"),
        F.count("unit_price").alias("unit_price_count"),
    )
)

result = (
    summary
    .filter(F.col("avg_quantity") >= 72)
    .select("quantity_bucket", "unit_price_count")
    .orderBy("quantity_bucket", F.col("unit_price_count").desc())
)

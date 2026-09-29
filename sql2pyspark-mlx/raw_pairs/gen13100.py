from pyspark.sql import functions as F

src = df_order_items.filter(F.col("unit_price").between(200, 280))

summary = (
    src
    .groupBy(
        F.when(F.col("quantity") >= 7, "high").when(F.col("quantity") >= 4, "mid").otherwise("low").alias("quantity_bucket"),
    )
    .agg((F.max("quantity") - F.min("quantity")).alias("quantity_range"))
)

result = (
    summary
    .select("quantity_bucket", "quantity_range")
    .orderBy("quantity_bucket", F.col("quantity_range").desc())
    .limit(10)
)

from pyspark.sql import functions as F

src = df_order_items.filter(F.col("order_id") == 5)

agg = (
    src
    .groupBy(
        F.when(F.col("unit_price") >= 420, "high").when(F.col("unit_price") >= 150, "mid").otherwise("low").alias("unit_price_bucket"),
        "order_id",
    )
    .agg(
        F.max("quantity").alias("max_quantity"),
        F.min("quantity").alias("min_quantity"),
    )
)

result = (
    agg
    .select("unit_price_bucket", "order_id", "max_quantity", "min_quantity")
    .orderBy("unit_price_bucket", F.col("order_id").desc())
)

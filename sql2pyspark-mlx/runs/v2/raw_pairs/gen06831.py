from pyspark.sql import functions as F

filtered_order_items = df_order_items.filter(F.col("quantity") > 3)

order_items_stats = (
    filtered_order_items
    .groupBy(
        "order_id",
        F.when(F.col("quantity") >= 9, "high").when(F.col("quantity") >= 2, "mid").otherwise("low").alias("quantity_bucket"),
    )
    .agg(F.round(F.avg("quantity"), 2).alias("avg_quantity"))
)

result = (
    order_items_stats
    .filter(F.col("avg_quantity") < 32)
    .select("order_id", "quantity_bucket", "avg_quantity")
    .orderBy(F.col("quantity_bucket").desc())
)

from pyspark.sql import functions as F

filtered_order_items = df_order_items.filter(F.col("unit_price") > 110)

order_items_stats = (
    filtered_order_items
    .groupBy(
        "order_id",
        F.when(F.col("quantity") >= 7, "high").when(F.col("quantity") >= 4, "mid").otherwise("low").alias("quantity_bucket"),
    )
    .agg((F.max("quantity") - F.min("quantity")).alias("quantity_range"))
)

result = (
    order_items_stats
    .filter(F.col("quantity_range") < 34)
    .select("order_id", "quantity_range")
    .orderBy(F.col("order_id").desc())
)

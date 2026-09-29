from pyspark.sql import functions as F

base = df_order_items.filter((F.col("unit_price") <= 270) | (F.col("unit_price") >= 270))

order_items_stats = (
    base
    .groupBy(
        F.when(F.col("quantity") >= 7, "high").when(F.col("quantity") >= 3, "mid").otherwise("low").alias("quantity_bucket"),
    )
    .agg(F.sum("unit_price").alias("total_unit_price"))
)

result = (
    order_items_stats
    .select("quantity_bucket", "total_unit_price")
    .orderBy("total_unit_price")
)

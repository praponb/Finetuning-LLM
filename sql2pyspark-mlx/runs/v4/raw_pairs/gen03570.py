from pyspark.sql import functions as F

recent_order_items = df_order_items.filter((F.col("quantity") >= 7) & F.col("order_id").isin([27, 37]))

summary = (
    recent_order_items
    .groupBy("order_id")
    .agg(
        F.avg("unit_price").alias("avg_unit_price"),
        F.min("quantity").alias("min_quantity"),
    )
)

result = (
    summary
    .filter(F.col("min_quantity") < 7.0)
    .select("order_id", "avg_unit_price", "min_quantity")
    .orderBy("min_quantity")
)

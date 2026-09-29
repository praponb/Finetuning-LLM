from pyspark.sql import functions as F

recent_order_items = df_order_items.filter(F.col("product_id").isNull() | (F.col("unit_price") > 340))

product_id_summary = (
    recent_order_items
    .groupBy("product_id", "order_id")
    .agg(
        F.avg("quantity").alias("avg_quantity"),
        (F.max("quantity") - F.min("quantity")).alias("quantity_range"),
    )
)

result = (
    product_id_summary
    .filter(F.col("quantity_range") >= 43)
    .orderBy("quantity_range")
)

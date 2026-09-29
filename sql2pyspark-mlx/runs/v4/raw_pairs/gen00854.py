from pyspark.sql import functions as F

valid_order_items = df_order_items.filter(F.col("unit_price") < 120)

summary = (
    valid_order_items
    .groupBy("product_id")
    .agg(
        F.avg("quantity").alias("avg_quantity"),
        (F.max("unit_price") - F.min("unit_price")).alias("unit_price_range"),
    )
)

result = summary

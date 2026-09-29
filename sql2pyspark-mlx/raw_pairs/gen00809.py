from pyspark.sql import functions as F

result = (
    df_order_items
    .filter((F.col("quantity") < 7) & (F.col("quantity") <= 4))
    .groupBy("product_id", "order_id")
    .agg(
        F.avg("quantity").alias("avg_quantity"),
        (F.max("unit_price") - F.min("unit_price")).alias("unit_price_range"),
    )
)

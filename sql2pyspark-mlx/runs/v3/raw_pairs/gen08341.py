from pyspark.sql import functions as F

result = (
    df_order_items
    .groupBy("order_id", "product_id")
    .agg(
        (F.max("unit_price") - F.min("unit_price")).alias("unit_price_range"),
        F.max("quantity").alias("max_quantity"),
    )
    .filter(F.col("max_quantity") > 5.5)
)

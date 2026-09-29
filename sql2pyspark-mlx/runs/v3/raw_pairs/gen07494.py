from pyspark.sql import functions as F

result = (
    df_order_items
    .filter((F.col("quantity") == 5) | (F.col("unit_price") >= 160))
    .agg(
        F.count("*").alias("order_item_count"),
        F.count("unit_price").alias("unit_price_count"),
        F.min("unit_price").alias("min_unit_price"),
    )
)

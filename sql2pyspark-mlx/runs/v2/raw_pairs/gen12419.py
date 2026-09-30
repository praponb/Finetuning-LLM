from pyspark.sql import functions as F

result = (
    df_order_items
    .groupBy("order_id")
    .agg(
        F.count("unit_price").alias("unit_price_count"),
        F.count("quantity").alias("quantity_count"),
        F.count("*").alias("order_item_count"),
    )
)

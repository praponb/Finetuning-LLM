from pyspark.sql import functions as F

result = (
    df_order_items
    .groupBy("order_id")
    .agg(
        F.count("unit_price").alias("unit_price_count"),
        F.count("quantity").alias("quantity_count"),
        F.max("unit_price").alias("max_unit_price"),
    )
)

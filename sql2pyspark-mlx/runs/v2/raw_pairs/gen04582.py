from pyspark.sql import functions as F

result = (
    df_order_items
    .select(
        "id",
        "order_id",
        F.ceil("quantity").alias("quantity_whole"),
        F.round("unit_price", 2).alias("unit_price_rounded"),
    )
)

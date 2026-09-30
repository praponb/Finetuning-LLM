from pyspark.sql import functions as F

result = (
    df_order_items
    .select(
        "id",
        "quantity",
        "order_id",
        F.ceil("quantity").alias("quantity_whole"),
        F.coalesce("unit_price", F.lit(0)).alias("unit_price_filled"),
    )
)

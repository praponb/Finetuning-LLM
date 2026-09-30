from pyspark.sql import functions as F

result = (
    df_order_items
    .select(
        "id",
        "unit_price",
        F.ceil("unit_price").alias("unit_price_whole"),
        F.ceil("unit_price").alias("unit_price_whole_2"),
    )
)

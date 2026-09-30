from pyspark.sql import functions as F

result = (
    df_order_items
    .select(
        "id",
        "order_id",
        "product_id",
        "quantity",
        F.ceil("unit_price").alias("unit_price_whole"),
    )
    .orderBy("unit_price_whole")
)

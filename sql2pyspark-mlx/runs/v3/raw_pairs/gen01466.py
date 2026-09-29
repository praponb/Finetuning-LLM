from pyspark.sql import functions as F

result = (
    df_order_items
    .select(
        "id",
        "unit_price",
        "quantity",
        "order_id",
        F.round((F.col("unit_price") * 100), 2).alias("unit_price_scaled"),
        F.ceil("unit_price").alias("unit_price_whole"),
    )
)

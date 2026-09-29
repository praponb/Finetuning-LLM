from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("quantity") == 3)
    .select(
        "id",
        "quantity",
        "unit_price",
        F.ceil("unit_price").alias("unit_price_whole"),
    )
)

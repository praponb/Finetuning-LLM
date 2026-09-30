from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("order_id") == 1)
    .select(
        "id",
        "product_id",
        "quantity",
        F.col("unit_price").cast("int").alias("unit_price_int"),
        F.floor("quantity").alias("quantity_whole"),
    )
)

from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("unit_price") <= 250)
    .select(
        "product_id",
        "quantity",
        F.col("quantity").cast("string").alias("quantity_num"),
    )
)

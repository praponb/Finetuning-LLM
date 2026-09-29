from pyspark.sql import functions as F

result = (
    df_order_items
    .select(
        "order_id",
        "quantity",
        F.col("unit_price").cast("int").alias("unit_price_int"),
    )
)

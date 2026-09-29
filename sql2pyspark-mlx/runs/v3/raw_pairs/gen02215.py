from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("order_id").isin([7, 29, 32]))
    .select(
        "id",
        "quantity",
        "order_id",
        "unit_price",
        F.col("unit_price").cast("int").alias("unit_price_int"),
    )
    .orderBy(F.col("id").desc(), "order_id")
)

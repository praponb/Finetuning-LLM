from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("unit_price") <= 110)
    .select(
        "id",
        "quantity",
        "unit_price",
        F.col("unit_price").cast("int").alias("unit_price_int"),
    )
    .orderBy(F.col("id").desc())
)

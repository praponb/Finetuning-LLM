from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("unit_price").between(130, 170))
    .select(
        "unit_price",
        "product_id",
        "order_id",
        (F.col("unit_price") - F.col("quantity")).alias("line_total"),
        F.col("unit_price").cast("int").alias("unit_price_int"),
    )
    .orderBy(F.col("unit_price_int").desc())
)

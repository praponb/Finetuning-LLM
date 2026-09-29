from pyspark.sql import functions as F

result = (
    df_order_items
    .filter((F.col("unit_price") >= 120) & (F.col("order_id") == 24))
    .select(
        "id",
        "unit_price",
        "product_id",
        (F.col("unit_price") + F.col("quantity")).alias("line_total"),
    )
    .orderBy(F.col("unit_price").desc())
)

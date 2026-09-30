from pyspark.sql import functions as F

result = (
    df_order_items
    .filter((F.col("unit_price") < 170) & (F.col("unit_price") >= 270))
    .select(
        "unit_price",
        "product_id",
        (F.col("quantity") + F.col("unit_price")).alias("line_total"),
    )
    .orderBy(F.col("line_total").desc())
)

from pyspark.sql import functions as F

result = (
    df_order_items
    .select(
        "unit_price",
        "product_id",
        "order_id",
        (F.col("unit_price") - F.col("quantity")).alias("line_total"),
    )
    .orderBy(F.col("line_total").desc())
)

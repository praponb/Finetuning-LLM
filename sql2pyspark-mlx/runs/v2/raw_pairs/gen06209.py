from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("product_id").isNotNull() & (F.col("unit_price") > 230))
    .select(
        "id",
        "unit_price",
        (F.col("unit_price") + F.col("quantity")).alias("line_total"),
    )
    .orderBy(F.col("line_total").desc(), "id")
    .limit(50)
)

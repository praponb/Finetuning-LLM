from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("product_id").isNull())
    .select(
        "quantity",
        "order_id",
        "unit_price",
        (F.col("unit_price") + F.col("quantity")).alias("line_total"),
    )
    .orderBy("line_total")
)

from pyspark.sql import functions as F

result = (
    df_order_items
    .filter((F.col("unit_price") >= 350) & F.col("product_id").isNull())
    .select(
        "order_id",
        "product_id",
        (F.col("unit_price") - F.col("quantity")).alias("line_total"),
    )
)

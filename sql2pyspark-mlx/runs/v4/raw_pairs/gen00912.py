from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("product_id").isin([9, 34]))
    .select(
        "unit_price",
        "quantity",
        "order_id",
        (F.col("unit_price") * F.col("quantity")).alias("line_total"),
    )
)

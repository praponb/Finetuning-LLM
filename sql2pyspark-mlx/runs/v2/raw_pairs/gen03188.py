from pyspark.sql import functions as F

result = (
    df_order_items
    .filter((F.col("quantity") <= 8) & F.col("order_id").isin([7, 19, 21, 34]))
    .select(
        "id",
        "unit_price",
        "order_id",
        "product_id",
        (F.col("quantity") * F.col("unit_price")).alias("line_total"),
    )
)

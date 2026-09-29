from pyspark.sql import functions as F

valid = (
    df_order_items
    .filter(F.col("product_id").isin([12, 14]))
    .select(
        "id",
        "order_id",
        "product_id",
        "quantity",
        "unit_price",
        (F.col("unit_price") * F.col("quantity")).alias("line_total"),
        (F.col("quantity") + F.col("unit_price")).alias("line_total_2"),
    )
)

result = (
    valid
    .select("product_id")
    .intersectAll(
        df_order_items
        .filter(F.col("order_id").isin([2, 4, 24]))
        .select("product_id")
    )
)

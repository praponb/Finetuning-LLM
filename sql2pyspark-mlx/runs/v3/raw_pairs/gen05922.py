from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("product_id").isNull() & (F.col("product_id").isin([9, 27, 36]) | (F.col("quantity") > 3)))
    .select(
        "id",
        "product_id",
        "quantity",
        (F.col("unit_price") * F.col("quantity")).alias("line_total"),
        (F.col("quantity") * F.col("unit_price")).alias("line_total_2"),
    )
)

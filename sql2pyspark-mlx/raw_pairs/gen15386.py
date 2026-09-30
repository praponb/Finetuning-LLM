from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("quantity") > 7)
    .select(
        "id",
        "unit_price",
        "product_id",
        "quantity",
        (F.col("unit_price") * F.col("quantity")).alias("line_total"),
    )
)

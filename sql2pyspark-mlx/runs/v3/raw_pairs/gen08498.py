from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("order_id").isin([2, 15, 20, 32]))
    .select(
        "id",
        "unit_price",
        "quantity",
        "order_id",
        (F.col("unit_price") + F.col("quantity")).alias("line_total"),
    )
)

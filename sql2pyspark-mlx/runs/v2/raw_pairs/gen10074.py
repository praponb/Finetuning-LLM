from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("quantity") > 4)
    .select(
        "id",
        "order_id",
        "unit_price",
        (F.col("quantity") + F.col("unit_price")).alias("line_total"),
        (F.col("unit_price") + F.col("quantity")).alias("line_total_2"),
    )
)

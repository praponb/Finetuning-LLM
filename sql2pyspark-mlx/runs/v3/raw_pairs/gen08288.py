from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("order_id").isin([16, 19, 28]))
    .select(
        "id",
        "order_id",
        "quantity",
        "unit_price",
        (F.col("quantity") + F.col("unit_price")).alias("line_total"),
        (F.col("quantity") - F.col("unit_price")).alias("line_total_2"),
    )
)

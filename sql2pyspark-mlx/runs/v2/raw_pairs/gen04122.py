from pyspark.sql import functions as F

result = (
    df_order_items
    .filter((F.col("quantity") == 8) & F.col("unit_price").between(200, 240))
    .select(
        "quantity",
        "order_id",
        (F.col("unit_price") * F.col("quantity")).alias("line_total"),
        (F.col("unit_price") + F.col("quantity")).alias("line_total_2"),
    )
)

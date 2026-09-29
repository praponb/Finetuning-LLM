from pyspark.sql import functions as F

result = (
    df_order_items
    .select(
        "id",
        "order_id",
        "quantity",
        (F.col("unit_price") - F.col("quantity")).alias("line_total"),
    )
)

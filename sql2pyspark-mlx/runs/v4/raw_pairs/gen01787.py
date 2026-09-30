from pyspark.sql import functions as F

result = (
    df_order_items
    .select(
        "id",
        "order_id",
        (F.col("unit_price") - F.col("quantity")).alias("line_total"),
        (F.col("quantity") - F.col("unit_price")).alias("line_total_2"),
    )
    .orderBy("id", "line_total")
    .limit(1)
)

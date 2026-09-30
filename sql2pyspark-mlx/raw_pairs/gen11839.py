from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("order_id").isin([4, 7, 29, 37]))
    .select(
        "id",
        "unit_price",
        (F.col("quantity") - F.col("unit_price")).alias("line_total"),
    )
    .orderBy("unit_price", "line_total", "id")
    .limit(10)
)

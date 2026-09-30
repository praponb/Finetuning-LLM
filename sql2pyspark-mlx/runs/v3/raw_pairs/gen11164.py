from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("order_id").isin([8, 21, 37]))
    .select(
        "id",
        "product_id",
        (F.col("quantity") - F.col("unit_price")).alias("line_total"),
    )
)

from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("product_id").isin([4, 7]) & (F.col("quantity") > 5))
    .select(
        "id",
        "unit_price",
        "product_id",
        (F.col("unit_price") * F.col("quantity")).alias("line_total"),
    )
)

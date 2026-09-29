from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("product_id").isin([11, 14]) & F.col("order_id").isin([7, 17]))
    .select(
        "id",
        "product_id",
        "order_id",
        "unit_price",
        F.round((F.col("unit_price") / F.col("quantity")), 2).alias("line_total"),
        (F.col("quantity") - F.col("unit_price")).alias("line_total_2"),
    )
)

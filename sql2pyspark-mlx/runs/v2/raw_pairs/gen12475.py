from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("product_id").isin([4, 14, 38]))
    .select(
        "id",
        "unit_price",
        "product_id",
        F.round((F.col("unit_price") / F.col("quantity")), 2).alias("line_total"),
    )
)

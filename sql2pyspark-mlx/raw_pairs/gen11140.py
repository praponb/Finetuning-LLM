from pyspark.sql import functions as F

filtered = (
    df_order_items
    .filter(F.col("product_id").isin([16, 26]))
    .select(
        "*",
        F.col("quantity").cast("double").alias("quantity_num"),
        F.round((F.col("unit_price") / F.col("quantity")), 2).alias("line_total"),
    )
)

result = (
    filtered
    .select("id", "product_id")
    .intersectAll(
        df_order_items
        .filter(F.col("order_id").isin([21, 24, 35, 36]))
        .select("id", "product_id")
    )
)

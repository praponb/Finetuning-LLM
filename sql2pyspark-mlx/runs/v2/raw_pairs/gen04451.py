from pyspark.sql import functions as F

result = (
    df_order_items
    .filter((F.col("order_id").isin([12, 24]) & F.col("order_id").isin([6, 21])) | (F.col("unit_price") <= 210))
    .select(
        "id",
        "unit_price",
        "order_id",
        "product_id",
        F.round((F.col("unit_price") / F.col("quantity")), 2).alias("line_total"),
        (F.col("unit_price") + F.col("quantity")).alias("line_total_2"),
    )
    .orderBy("id")
)

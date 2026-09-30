from pyspark.sql import functions as F

result = (
    df_order_items
    .filter((F.col("product_id") == 25) & F.col("product_id").isin([19, 26, 28]) & F.col("product_id").isNull())
    .select(
        "id",
        "product_id",
        "unit_price",
        "order_id",
        F.ceil("unit_price").alias("unit_price_whole"),
        (F.col("unit_price") + F.col("quantity")).alias("line_total"),
    )
)

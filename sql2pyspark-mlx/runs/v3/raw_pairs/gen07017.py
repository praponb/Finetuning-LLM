from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("unit_price") <= 380)
    .select(
        "product_id",
        "unit_price",
        "order_id",
        F.coalesce("unit_price", F.lit(0)).alias("unit_price_filled"),
        (F.col("unit_price") * F.col("quantity")).alias("line_total"),
    )
    .orderBy("order_id", F.col("product_id").desc())
)

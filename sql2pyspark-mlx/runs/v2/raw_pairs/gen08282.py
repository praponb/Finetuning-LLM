from pyspark.sql import functions as F

result = (
    df_order_items
    .select(
        "unit_price",
        "product_id",
        (F.col("quantity") * F.col("unit_price")).alias("line_total"),
        F.floor("unit_price").alias("unit_price_whole"),
    )
)

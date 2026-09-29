from pyspark.sql import functions as F

result = (
    df_order_items
    .select(
        "id",
        "product_id",
        (F.col("unit_price") - F.col("quantity")).alias("line_total"),
        F.coalesce("unit_price", F.lit(0)).alias("unit_price_filled"),
    )
)

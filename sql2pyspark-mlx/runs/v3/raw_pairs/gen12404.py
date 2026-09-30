from pyspark.sql import functions as F

result = (
    df_order_items
    .filter((F.col("quantity") != 4) & ((F.col("order_id") == 26) | (F.col("unit_price") <= 210)))
    .select(
        "unit_price",
        "product_id",
        F.coalesce("quantity", F.lit(0)).alias("quantity_filled"),
        F.round((F.col("unit_price") / F.col("quantity")), 2).alias("line_total"),
    )
    .orderBy("product_id")
)

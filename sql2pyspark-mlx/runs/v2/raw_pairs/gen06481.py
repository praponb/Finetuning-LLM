from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(((F.col("quantity") != 8) & (F.col("product_id") == 3)) | F.col("quantity").between(5, 7))
    .select(
        "quantity",
        "order_id",
        F.round("unit_price", 0).alias("unit_price_rounded"),
        F.coalesce("unit_price", F.lit(0)).alias("unit_price_filled"),
    )
)

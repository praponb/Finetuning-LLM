from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(((F.col("quantity") >= 7) & (F.col("unit_price") <= 180)) | (F.col("order_id") == 38))
    .select(
        "id",
        "product_id",
        "unit_price",
        F.coalesce("quantity", F.lit(0)).alias("quantity_filled"),
    )
)

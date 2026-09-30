from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("product_id").isNotNull() & F.col("product_id").isNotNull())
    .select(
        "id",
        "product_id",
        "quantity",
        F.coalesce("unit_price", F.lit(0)).alias("unit_price_filled"),
    )
)

from pyspark.sql import functions as F

result = (
    df_order_items
    .select(
        "id",
        "order_id",
        "unit_price",
        "product_id",
        F.coalesce("unit_price", F.lit(0)).alias("unit_price_filled"),
    )
)

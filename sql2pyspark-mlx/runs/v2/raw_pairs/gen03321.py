from pyspark.sql import functions as F

result = (
    df_order_items
    .select(
        "id",
        "quantity",
        "product_id",
        F.round("unit_price", 0).alias("unit_price_rounded"),
    )
)

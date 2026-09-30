from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("unit_price").between(210, 220))
    .select(
        "id",
        "product_id",
        "quantity",
        "unit_price",
        F.round("unit_price", 1).alias("unit_price_rounded"),
    )
)

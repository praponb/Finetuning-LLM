from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("unit_price").between(210, 340))
    .select(
        "id",
        "order_id",
        "unit_price",
        "quantity",
        F.round("unit_price", 0).alias("unit_price_rounded"),
    )
)

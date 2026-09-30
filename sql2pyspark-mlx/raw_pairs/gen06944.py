from pyspark.sql import functions as F

result = (
    df_order_items
    .select(
        "id",
        "order_id",
        "quantity",
        F.least("unit_price", "quantity").alias("least_unit_price_quantity"),
        F.round("quantity", 0).alias("quantity_rounded"),
    )
)

from pyspark.sql import functions as F

result = (
    df_order_items
    .select(
        "quantity",
        "order_id",
        "unit_price",
        F.least("unit_price", "quantity").alias("least_unit_price_quantity"),
    )
)

from pyspark.sql import functions as F

result = (
    df_order_items
    .select(
        "id",
        "product_id",
        F.least("unit_price", "quantity").alias("least_unit_price_quantity"),
    )
)

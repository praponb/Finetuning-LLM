from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("order_id") == 30)
    .select(
        "id",
        "unit_price",
        "quantity",
        "product_id",
        F.least("unit_price", "quantity").alias("least_unit_price_quantity"),
    )
    .orderBy("quantity")
)

from pyspark.sql import functions as F

result = (
    df_order_items
    .select(
        "id",
        "quantity",
        "product_id",
        "order_id",
        F.least("quantity", "unit_price").alias("least_quantity_unit_price"),
    )
    .orderBy(F.col("quantity").desc())
)

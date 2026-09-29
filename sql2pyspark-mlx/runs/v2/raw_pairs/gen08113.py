from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("quantity") != 4)
    .select(
        "quantity",
        "order_id",
        "product_id",
        F.greatest("quantity", "unit_price").alias("greatest_quantity_unit_price"),
    )
)

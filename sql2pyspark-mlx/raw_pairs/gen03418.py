from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("quantity").between(5, 7))
    .select(
        "id",
        "quantity",
        "unit_price",
        F.least("unit_price", "quantity").alias("least_unit_price_quantity"),
    )
    .orderBy("least_unit_price_quantity", "id")
    .limit(3)
)

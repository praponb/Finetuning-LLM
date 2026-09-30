from pyspark.sql import functions as F

result = (
    df_order_items
    .filter((F.col("unit_price") < 220) & F.col("unit_price").isNotNull() & (F.col("quantity") == 6))
    .select(
        "id",
        "unit_price",
        F.least("quantity", "unit_price").alias("least_quantity_unit_price"),
    )
)

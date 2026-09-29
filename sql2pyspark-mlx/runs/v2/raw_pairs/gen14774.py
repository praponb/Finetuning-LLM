from pyspark.sql import functions as F

result = (
    df_order_items
    .filter((F.col("unit_price") > 110) & (F.col("unit_price") >= 220))
    .select(
        "id",
        "unit_price",
        "quantity",
        F.least("quantity", "unit_price").alias("least_quantity_unit_price"),
    )
)

from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("unit_price") > 200)
    .select(
        "id",
        "product_id",
        F.least("quantity", "unit_price").alias("least_quantity_unit_price"),
    )
    .orderBy("id")
)

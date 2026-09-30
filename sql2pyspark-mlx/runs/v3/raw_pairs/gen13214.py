from pyspark.sql import functions as F

result = (
    df_order_items
    .select(
        "id",
        "quantity",
        "order_id",
        "unit_price",
        F.least("quantity", "unit_price").alias("least_quantity_unit_price"),
        F.round((F.col("unit_price") * 1.1), 2).alias("unit_price_scaled"),
    )
)

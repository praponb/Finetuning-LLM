from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("order_id").isin([11, 15, 40]) & F.col("product_id").isNull() & F.col("product_id").isin([11, 12, 15, 25]))
    .select(
        "product_id",
        "order_id",
        "unit_price",
        F.least("unit_price", "quantity").alias("least_unit_price_quantity"),
        F.round((F.col("unit_price") / F.col("quantity")), 2).alias("line_total"),
    )
    .orderBy("least_unit_price_quantity", F.col("unit_price").desc())
)

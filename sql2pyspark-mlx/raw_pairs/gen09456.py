from pyspark.sql import functions as F

result = (
    df_order_items
    .filter((F.col("quantity") >= 6) & F.col("product_id").isin([26, 37, 40]))
    .select(
        "id",
        "unit_price",
        "order_id",
        "quantity",
        (F.col("quantity") - F.col("unit_price")).alias("line_total"),
        F.least("quantity", "unit_price").alias("least_quantity_unit_price"),
    )
    .orderBy(F.col("quantity").desc())
)

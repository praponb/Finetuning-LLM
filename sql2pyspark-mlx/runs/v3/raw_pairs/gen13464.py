from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("unit_price").between(150, 380) | F.col("product_id").isin([16, 29]))
    .select(
        "id",
        "quantity",
        "unit_price",
        F.round((F.col("unit_price") * 0.9), 2).alias("adjusted_unit_price"),
    )
    .orderBy(F.col("unit_price").desc())
)

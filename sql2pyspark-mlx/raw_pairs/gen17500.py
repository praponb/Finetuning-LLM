from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("order_id").isin([1, 2, 9, 16]) & F.col("product_id").isNull())
    .select(
        "id",
        "order_id",
        "unit_price",
        F.round((F.col("unit_price") * 0.85), 2).alias("unit_price_scaled"),
    )
    .orderBy(F.col("order_id").desc())
)

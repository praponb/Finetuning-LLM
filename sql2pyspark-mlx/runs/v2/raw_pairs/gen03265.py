from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("product_id").isin([4, 25, 37]) & ((F.col("quantity") != 7) | (F.col("order_id") == 19)))
    .select(
        "id",
        "product_id",
        "unit_price",
        F.round((F.col("unit_price") * 1.1), 2).alias("unit_price_scaled"),
    )
    .orderBy("product_id", F.col("unit_price").desc())
)

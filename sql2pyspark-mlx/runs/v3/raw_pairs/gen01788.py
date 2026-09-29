from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("order_id").isin([5, 9, 11, 35]))
    .select(
        "id",
        "quantity",
        "product_id",
        F.round((F.col("unit_price") * 0.85), 2).alias("unit_price_scaled"),
    )
)

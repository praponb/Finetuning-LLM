from pyspark.sql import functions as F

result = (
    df_order_items
    .filter((F.col("quantity") >= 6) | (F.col("quantity") > 5))
    .select(
        "id",
        "order_id",
        F.round((F.col("unit_price") * 0.9), 2).alias("unit_price_scaled"),
    )
)

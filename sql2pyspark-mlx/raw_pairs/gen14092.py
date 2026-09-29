from pyspark.sql import functions as F

result = (
    df_order_items
    .filter((F.col("unit_price") <= 210) | (F.col("quantity") < 5))
    .select(
        "unit_price",
        "order_id",
        F.round("quantity", 0).alias("quantity_rounded"),
        F.round((F.col("quantity") * 1.15), 2).alias("quantity_scaled"),
    )
)

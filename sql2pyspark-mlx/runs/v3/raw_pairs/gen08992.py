from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("order_id").isin([15, 26, 38]) & (F.col("order_id").isin([13, 22, 27, 37]) | F.col("order_id").isin([1, 12, 24])))
    .select(
        "id",
        "unit_price",
        F.ceil("quantity").alias("quantity_whole"),
        F.round("quantity", 1).alias("quantity_rounded"),
    )
)

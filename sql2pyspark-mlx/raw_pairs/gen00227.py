from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("order_id").isin([3, 18, 36]) & (F.col("quantity") < 3))
    .select(
        "id",
        "order_id",
        "product_id",
        "quantity",
        F.round("quantity", 0).alias("quantity_rounded"),
    )
)

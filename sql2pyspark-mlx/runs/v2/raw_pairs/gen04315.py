from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("order_id").isin([13, 14, 38]))
    .select(
        "id",
        "unit_price",
        "order_id",
        F.round("quantity", 1).alias("quantity_rounded"),
    )
)

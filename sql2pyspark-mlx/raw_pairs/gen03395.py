from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("order_id").isin([20, 34]) | (F.col("order_id") == 24))
    .select(
        "id",
        "unit_price",
        F.round((F.col("unit_price") / F.col("quantity")), 2).alias("line_total"),
    )
)

from pyspark.sql import functions as F

result = (
    df_order_items
    .select(
        "id",
        "order_id",
        (F.col("unit_price") * F.col("quantity")).alias("line_total"),
        F.round((F.col("unit_price") * 1.15), 2).alias("unit_price_scaled"),
    )
)

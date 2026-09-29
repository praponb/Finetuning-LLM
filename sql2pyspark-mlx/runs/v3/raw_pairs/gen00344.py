from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("order_id").isin([21, 37]) & (F.col("unit_price") >= 210))
    .select(
        "id",
        "order_id",
        F.least("unit_price", "quantity").alias("least_unit_price_quantity"),
    )
)

from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("order_id").isin([16, 38]) & (F.col("order_id").isin([4, 31]) | (F.col("quantity") > 4)))
    .select(
        "id",
        "unit_price",
        F.least("unit_price", "quantity").alias("least_unit_price_quantity"),
    )
)

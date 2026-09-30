from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(~(F.col("order_id").isin([32, 34, 40]) & (F.col("quantity") == 3)))
    .select(
        "unit_price",
        "quantity",
        F.round((F.col("unit_price") * 1.15), 2).alias("unit_price_adjusted"),
    )
)

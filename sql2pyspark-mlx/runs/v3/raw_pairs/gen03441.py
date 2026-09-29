from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("product_id").isNull() & F.col("order_id").isin([6, 31, 32, 38]))
    .select(
        "order_id",
        F.when(F.col("unit_price") >= 380, "High").when(F.col("unit_price") >= 110, "Medium").otherwise("Low").alias("unit_price_level"),
    )
)

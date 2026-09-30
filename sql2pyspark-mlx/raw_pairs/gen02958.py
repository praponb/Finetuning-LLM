from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("product_id").isNull() & (F.col("product_id").isin([25, 35, 36]) | F.col("quantity").between(4, 6)))
    .select(
        "unit_price",
        F.when(F.col("unit_price") >= 350, "High").when(F.col("unit_price") >= 130, "Medium").otherwise("Low").alias("unit_price_tier"),
    )
)

from pyspark.sql import functions as F

result = (
    df_order_items
    .filter((F.col("quantity") != 5) & (F.col("product_id").isin([10, 14, 22]) | (F.col("unit_price") < 140)))
    .select(
        "id",
        "unit_price",
        F.when(F.col("quantity") >= 8, "High").when(F.col("quantity") >= 3, "Medium").otherwise("Low").alias("quantity_tier"),
    )
)

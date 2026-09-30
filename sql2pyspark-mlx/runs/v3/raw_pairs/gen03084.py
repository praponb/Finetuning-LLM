from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("product_id").isin([3, 5, 37]))
    .select(
        "id",
        "order_id",
        "product_id",
        F.when(F.col("unit_price") >= 390, "High").when(F.col("unit_price") >= 190, "Medium").otherwise("Low").alias("unit_price_tier"),
    )
)

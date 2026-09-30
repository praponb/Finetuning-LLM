from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("product_id").isin([3, 9]))
    .select(
        "id",
        "order_id",
        "unit_price",
        F.when(F.col("quantity") >= 7, "High").when(F.col("quantity") >= 2, "Medium").otherwise("Low").alias("quantity_tier"),
    )
)

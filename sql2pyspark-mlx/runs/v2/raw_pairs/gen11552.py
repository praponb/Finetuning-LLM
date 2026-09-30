from pyspark.sql import functions as F

result = (
    df_order_items
    .select(
        "id",
        "product_id",
        "unit_price",
        F.when(F.col("quantity") >= 8, "High").when(F.col("quantity") >= 4, "Medium").otherwise("Low").alias("quantity_band"),
    )
)

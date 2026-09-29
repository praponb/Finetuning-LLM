from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("quantity") <= 4)
    .select(
        "quantity",
        "unit_price",
        F.when(F.col("quantity") >= 9, "High").when(F.col("quantity") >= 4, "Medium").otherwise("Low").alias("quantity_level"),
    )
    .orderBy(F.col("unit_price").desc())
)

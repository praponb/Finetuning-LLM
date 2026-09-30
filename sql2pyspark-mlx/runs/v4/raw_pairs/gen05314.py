from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("quantity").between(200, 215) & (F.col("store_id") == 6) & (F.col("quantity") <= 229))
    .select(
        "id",
        "product_id",
        F.when(F.col("quantity") >= 239, "High").when(F.col("quantity") >= 49, "Medium").otherwise("Low").alias("quantity_level"),
    )
)

from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("quantity") > 159)
    .select(
        "product_id",
        "store_id",
        "last_restock",
        F.when(F.col("quantity") >= 246, "High").when(F.col("quantity") >= 116, "Medium").otherwise("Low").alias("quantity_band"),
        F.when(F.col("quantity") >= 219, "High").when(F.col("quantity") >= 58, "Medium").otherwise("Low").alias("quantity_band_2"),
    )
)

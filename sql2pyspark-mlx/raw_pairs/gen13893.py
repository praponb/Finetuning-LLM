from pyspark.sql import functions as F

result = (
    df_inventory
    .select(
        "store_id",
        "last_restock",
        F.when(F.col("quantity") >= 217, "High").when(F.col("quantity") >= 32, "Medium").otherwise("Low").alias("quantity_band"),
    )
    .orderBy(F.col("store_id").desc())
)

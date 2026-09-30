from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(~(F.col("product_id").isin([5, 7, 10]) & (F.year("last_restock") == 2024) & (F.col("quantity") < 60)))
    .select(
        "store_id",
        "last_restock",
        F.when(F.col("quantity") >= 195, "High").when(F.col("quantity") >= 45, "Medium").otherwise("Low").alias("quantity_band"),
    )
)

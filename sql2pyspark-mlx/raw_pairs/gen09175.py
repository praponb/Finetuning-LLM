from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("last_restock") <= "2025-06-01")
    .select(
        "id",
        "product_id",
        "store_id",
        F.when(F.col("quantity") >= 232, "High").when(F.col("quantity") >= 74, "Medium").otherwise("Low").alias("quantity_band"),
    )
    .orderBy(F.col("store_id").desc(), "quantity_band")
)

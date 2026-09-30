from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("product_id").isin([1, 2, 6, 8]) & F.col("store_id").isin([1, 3, 4, 5]))
    .select(
        "id",
        "quantity",
        F.when(F.col("quantity") >= 184, "High").when(F.col("quantity") >= 45, "Medium").otherwise("Low").alias("quantity_band"),
        F.date_format("last_restock", "yyyy-MM-dd").alias("last_restock_label"),
    )
)

from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("product_id").isin([4, 5]) & F.col("product_id").isin([5, 7]))
    .select(
        "id",
        "product_id",
        F.date_format("last_restock", "yyyy-MM-dd").alias("last_restock_label"),
        F.when(F.col("quantity") >= 181, "High").when(F.col("quantity") >= 100, "Medium").otherwise("Low").alias("quantity_band"),
    )
)

from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("store_id").isin([1, 2, 4, 6]) & (F.col("store_id").isin([2, 4, 6]) | (F.col("product_id") == 1)))
    .select(
        "id",
        "store_id",
        "quantity",
        F.date_trunc("year", "last_restock").alias("last_restock_period"),
        F.when(F.col("quantity") >= 185, "High").when(F.col("quantity") >= 103, "Medium").otherwise("Low").alias("quantity_tier"),
    )
)

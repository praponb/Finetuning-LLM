from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("product_id").isin([5, 7]) & (F.col("product_id") == 2) & (F.col("last_restock") > "2024-10-01"))
    .select(
        "store_id",
        "last_restock",
        F.when(F.col("quantity") >= 260, "High").when(F.col("quantity") >= 40, "Medium").otherwise("Low").alias("quantity_level"),
        F.when(F.col("quantity") >= 267, "High").when(F.col("quantity") >= 102, "Medium").otherwise("Low").alias("quantity_level_2"),
    )
    .orderBy(F.col("quantity_level").desc(), "quantity_level_2")
)

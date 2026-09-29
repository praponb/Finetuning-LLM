from pyspark.sql import functions as F

result = (
    df_inventory
    .select(
        "id",
        "store_id",
        "last_restock",
        "product_id",
        F.when(F.col("quantity") >= 247, "High").when(F.col("quantity") >= 50, "Medium").otherwise("Low").alias("quantity_level"),
    )
    .orderBy(F.col("product_id").desc(), "id")
    .limit(5)
)

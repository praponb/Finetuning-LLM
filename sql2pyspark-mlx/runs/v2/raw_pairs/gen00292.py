from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("quantity").between(174, 196))
    .select(
        "id",
        "store_id",
        "last_restock",
        "product_id",
        F.when(F.col("quantity") >= 210, "High").when(F.col("quantity") >= 44, "Medium").otherwise("Low").alias("quantity_tier"),
    )
)

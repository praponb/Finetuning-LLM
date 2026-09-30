from pyspark.sql import functions as F

valid = (
    df_inventory
    .filter(F.col("product_id").isin([4, 10]))
    .select(
        "id",
        "store_id",
        "product_id",
        "last_restock",
        F.when(F.col("quantity") >= 242, "High").when(F.col("quantity") >= 98, "Medium").otherwise("Low").alias("quantity_tier"),
    )
)

result = (
    valid
    .select("id", "store_id")
    .intersectAll(
        df_inventory
        .filter(F.year("last_restock") >= 2024)
        .select("id", "store_id")
    )
)

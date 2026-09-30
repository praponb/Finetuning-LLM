from pyspark.sql import functions as F

result = (
    df_inventory
    .filter((F.col("quantity") != 188) & (F.col("product_id") == 6) & F.col("last_restock").between("2023-03-01", "2024-11-13"))
    .select(
        "id",
        "store_id",
        F.when(F.col("quantity") >= 189, "High").when(F.col("quantity") >= 63, "Medium").otherwise("Low").alias("quantity_level"),
    )
    .orderBy(F.col("store_id").desc())
)

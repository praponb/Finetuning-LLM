from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("last_restock").between("2023-04-01", "2024-04-01"))
    .groupBy("store_id")
    .agg(
        F.countDistinct("quantity").alias("unique_products"),
        F.countDistinct("quantity").alias("unique_stores"),
        (F.max("quantity") - F.min("quantity")).alias("quantity_range"),
    )
    .filter(F.col("quantity_range") < 60)
    .orderBy(F.col("unique_stores").desc())
)

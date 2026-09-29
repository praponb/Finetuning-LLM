from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_stores.alias("s"), F.col("i.store_id") == F.col("s.id"), "inner")
    .filter(F.col("i.last_restock").between("2023-10-17", "2024-07-01"))
    .groupBy(
        F.when(F.col("s.sqft") >= 14154, "high").when(F.col("s.sqft") >= 4190, "mid").otherwise("low").alias("sqft_bucket"),
    )
    .agg(
        F.max(F.col("i.last_restock")).alias("last_last_restock"),
        F.min(F.col("i.quantity")).alias("min_quantity"),
    )
    .orderBy(F.col("min_quantity").desc())
)

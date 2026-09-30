from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("store_id").isin([1, 6]) & F.col("quantity").isNotNull())
    .groupBy(
        "product_id",
        "store_id",
        F.month("last_restock").alias("last_restock_month"),
    )
    .agg(F.max("last_restock").alias("last_last_restock"))
    .orderBy(F.col("last_restock_month").desc())
)

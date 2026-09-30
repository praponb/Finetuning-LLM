from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("store_id").isin([1, 4]) & F.col("quantity").isNotNull())
    .groupBy(
        "store_id",
        "product_id",
        F.year("last_restock").alias("last_restock_year"),
    )
    .agg(F.avg("quantity").alias("avg_quantity"))
    .orderBy(F.col("last_restock_year").desc())
)

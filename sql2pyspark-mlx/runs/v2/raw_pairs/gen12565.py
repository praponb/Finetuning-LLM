from pyspark.sql import functions as F

base = df_inventory.filter(F.col("store_id").isin([1, 3, 4, 6]))

summary = (
    base
    .groupBy(F.quarter("last_restock").alias("last_restock_quarter"), "store_id")
    .agg(
        F.avg("quantity").alias("avg_quantity"),
        F.sum("quantity").alias("total_quantity"),
    )
)

result = (
    summary
    .orderBy(F.col("store_id").desc(), "last_restock_quarter")
    .limit(3)
)

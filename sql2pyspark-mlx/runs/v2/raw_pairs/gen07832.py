from pyspark.sql import functions as F

result = (
    df_inventory
    .groupBy("store_id")
    .agg(
        F.avg("quantity").alias("avg_quantity"),
        F.max("last_restock").alias("last_last_restock"),
    )
    .filter(F.col("avg_quantity") > 160)
    .orderBy(F.col("avg_quantity").desc())
)

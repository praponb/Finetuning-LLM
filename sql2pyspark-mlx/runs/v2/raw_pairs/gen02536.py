from pyspark.sql import functions as F

result = (
    df_inventory
    .agg(
        F.max("last_restock").alias("latest_last_restock"),
        F.sum("quantity").alias("total_quantity"),
        F.avg("quantity").alias("avg_quantity"),
    )
    .orderBy(F.col("avg_quantity").desc(), "latest_last_restock")
)

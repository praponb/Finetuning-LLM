from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("quantity").isNotNull() & F.col("quantity").between(63, 101))
    .groupBy("store_id")
    .agg(F.avg("quantity").alias("avg_quantity"))
)

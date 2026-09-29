from pyspark.sql import functions as F

base = (
    df_inventory
    .filter(F.col("quantity").between(92, 97) | F.col("last_restock").between("2023-04-01", "2023-05-01"))
    .withColumn("quantity_num", F.col("quantity").cast("string"))
)

summary = (
    base
    .groupBy("store_id")
    .agg(F.round(F.avg("quantity"), 2).alias("avg_quantity"))
)

result = (
    summary
    .select("store_id", "avg_quantity")
    .orderBy("avg_quantity", F.col("store_id").desc())
)

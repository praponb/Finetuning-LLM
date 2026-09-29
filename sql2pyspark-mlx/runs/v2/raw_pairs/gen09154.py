from pyspark.sql import functions as F

result = (
    df_inventory
    .filter((F.col("quantity") != 152) | (F.col("last_restock") >= "2023-09-07"))
    .withColumn("adjusted_quantity", F.round((F.col("quantity") * 1.15), 2))
    .withColumn("quantity_scaled", F.round((F.col("quantity") * 0.9), 2))
    .orderBy(F.col("store_id").desc(), "id")
    .limit(1)
)

from pyspark.sql import functions as F

result = (
    df_inventory
    .filter((F.col("quantity").between(182, 222) & (F.col("last_restock") <= "2025-02-03")) | (F.col("quantity") <= 109))
    .withColumn("quantity_filled", F.coalesce("quantity", F.lit(0)))
    .withColumn("quantity_rounded", F.round("quantity", 1))
    .orderBy("last_restock", "id")
    .limit(50)
)

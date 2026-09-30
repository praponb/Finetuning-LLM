from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("quantity") != 81)
    .groupBy("store_id", F.year("last_restock").alias("last_restock_year"))
    .agg(F.max("last_restock").alias("last_last_restock"))
    .orderBy("last_restock_year", "store_id")
    .limit(1)
)

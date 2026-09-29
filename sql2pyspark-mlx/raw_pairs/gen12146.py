from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("store_id").isin([1, 3, 5, 6]) & (F.year("last_restock") == 2025))
    .withColumn("last_restock_label", F.date_format("last_restock", "MMM yyyy"))
    .orderBy("last_restock")
)

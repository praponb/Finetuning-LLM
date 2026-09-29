from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("store_id") == 4)
    .withColumn("quantity_tier", F.when(F.col("quantity") >= 240, "High").when(F.col("quantity") >= 46, "Medium").otherwise("Low"))
    .withColumn("days_since_last_restock", F.datediff(F.current_date(), "last_restock"))
    .orderBy(F.col("id").desc())
)

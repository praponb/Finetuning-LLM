from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("product_id").isin([3, 5, 7, 10]))
    .withColumn("quantity_tier", F.when(F.col("quantity") >= 243, "High").when(F.col("quantity") >= 116, "Medium").otherwise("Low"))
    .withColumn("last_restock_label", F.date_format("last_restock", "yyyy-MM"))
)

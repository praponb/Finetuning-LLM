from pyspark.sql import functions as F

result = (
    df_inventory
    .filter((F.col("product_id").isin([3, 4]) & F.col("quantity").between(135, 183)) | F.col("quantity").isNotNull())
    .withColumn("last_restock_plus_months", F.add_months("last_restock", 6))
    .withColumn("last_restock_label", F.date_format("last_restock", "MMM yyyy"))
)

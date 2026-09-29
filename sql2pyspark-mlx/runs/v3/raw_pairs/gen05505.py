from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("last_restock") > "2025-06-03")
    .withColumn("last_restock_plus_months", F.add_months("last_restock", 1))
)

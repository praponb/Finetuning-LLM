from pyspark.sql import functions as F

result = df_inventory.agg(F.max("last_restock").alias("latest_last_restock"))

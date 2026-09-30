from pyspark.sql import functions as F

result = df_inventory.agg(F.min("quantity").alias("min_quantity"))

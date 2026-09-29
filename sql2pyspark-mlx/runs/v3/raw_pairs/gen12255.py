from pyspark.sql import functions as F

result = df_inventory.filter(F.col("quantity") != 236)

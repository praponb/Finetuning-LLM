from pyspark.sql import functions as F

result = df_inventory.filter(F.col("last_restock") <= "2023-03-01")

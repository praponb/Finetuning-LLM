from pyspark.sql import functions as F

result = df_inventory.filter(F.col("quantity").between(93, 101) & (F.col("quantity") < 74))

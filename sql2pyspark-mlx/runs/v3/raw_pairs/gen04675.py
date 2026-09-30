from pyspark.sql import functions as F

result = df_order_items.filter(F.col("quantity").between(5, 7))

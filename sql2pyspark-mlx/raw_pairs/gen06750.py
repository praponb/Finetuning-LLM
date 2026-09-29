from pyspark.sql import functions as F

result = df_products.agg((F.max("cost") - F.min("cost")).alias("cost_range"))

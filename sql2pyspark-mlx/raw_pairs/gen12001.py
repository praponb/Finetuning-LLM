from pyspark.sql import functions as F

result = df_order_items.withColumn("quantity_rounded", F.round("quantity", 0))

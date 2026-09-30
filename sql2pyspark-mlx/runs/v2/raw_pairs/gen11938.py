from pyspark.sql import functions as F

result = df_order_items.filter(F.col("product_id").isin([3, 7, 28, 32]))

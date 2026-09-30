from pyspark.sql import functions as F

result = df_orders.orderBy(F.col("order_date").desc(), F.col("id").desc()).limit(10)

from pyspark.sql import functions as F

result = df_sessions.groupBy("device").agg(F.countDistinct("customer_id").alias("unique_visitors"))

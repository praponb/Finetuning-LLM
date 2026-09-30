from pyspark.sql import functions as F

result = df_sessions.agg(F.countDistinct("customer_id").alias("active_customers"))

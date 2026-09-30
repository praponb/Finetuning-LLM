from pyspark.sql import functions as F

result = df_subscriptions.agg(F.avg("monthly_fee").alias("avg_monthly_fee"))

from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.month("start_date").isin([2, 3, 12]))
    .agg(F.max("end_date").alias("latest_end_date"))
)

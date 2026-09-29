from pyspark.sql import functions as F

result = (
    df_subscriptions
    .withColumn("start_date_label", F.date_format("start_date", "yyyy-MM-dd"))
    .withColumn("start_date_period", F.date_trunc("week", "start_date"))
)

from pyspark.sql import functions as F

result = (
    df_subscriptions
    .select("id", "status", F.date_add("end_date", 56).alias("end_date_plus_56d"))
)

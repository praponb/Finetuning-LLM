from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.col("monthly_fee") < 80)
    .select("id", "status", F.date_sub("end_date", 18).alias("end_date_minus_18d"))
    .orderBy(F.col("status").desc())
)

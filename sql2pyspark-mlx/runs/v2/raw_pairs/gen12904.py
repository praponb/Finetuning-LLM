from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.col("status") != "EXPIRED")
    .withColumn("monthly_fee_rounded", F.round("monthly_fee", 0))
    .withColumn("start_date_quarter", F.quarter("start_date"))
)

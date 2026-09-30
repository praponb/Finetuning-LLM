from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.col("plan") == "premium")
    .withColumn("start_date_month", F.month("start_date"))
    .withColumn("start_date_period", F.date_trunc("MONTH", "start_date"))
    .orderBy(F.col("id").desc())
)

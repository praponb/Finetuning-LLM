from pyspark.sql import functions as F

result = (
    df_subscriptions
    .select("monthly_fee", "end_date", F.trim("plan").alias("clean_plan"))
    .orderBy("end_date")
)

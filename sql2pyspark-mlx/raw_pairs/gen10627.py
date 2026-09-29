from pyspark.sql import functions as F

result = (
    df_subscriptions
    .select("id", "start_date", "plan")
    .orderBy("start_date", F.col("id").desc())
    .limit(10)
)

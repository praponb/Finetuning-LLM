from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.col("monthly_fee") <= 200)
    .select("id", "start_date", "end_date")
    .orderBy(F.col("id").desc())
)

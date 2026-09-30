from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.col("start_date") >= "2024-07-17")
    .select("id", "plan", "status")
)

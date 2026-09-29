from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(~(F.col("plan").isin(["premium", "basic", "standard"]) & (F.col("customer_id") == 35) & (F.col("start_date") < "2024-07-01")))
    .withColumn("clean_status", F.trim("status"))
    .withColumn("clean_status_2", F.trim("status"))
)

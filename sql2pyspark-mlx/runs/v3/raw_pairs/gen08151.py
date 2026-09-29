from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.col("start_date") >= "2024-06-10")
    .withColumn("status_plan_key", F.concat_ws(",", "status", "plan"))
    .orderBy(F.col("customer_id").desc(), "status_plan_key", "id")
    .limit(5)
)

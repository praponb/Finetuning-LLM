from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter((F.col("customer_id") == 2) & (F.col("status").isin(["PAUSED", "CANCELLED"]) | (F.col("start_date") < "2023-06-01")))
    .withColumn("clean_plan", F.trim("plan"))
    .withColumn("plan_title", F.initcap(F.trim("plan")))
    .orderBy("clean_plan", "id")
    .limit(5)
)

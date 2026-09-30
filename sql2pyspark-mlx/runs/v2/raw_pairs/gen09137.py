from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.col("customer_id").isNull() & (F.col("plan") != "enterprise"))
    .withColumn("plan_status", F.concat("plan", F.lit("/"), "status"))
    .withColumn("status_plan", F.concat("status", F.lit("/"), "plan"))
    .orderBy(F.col("customer_id").desc())
)

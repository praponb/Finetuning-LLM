from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.col("plan") == "enterprise")
    .withColumn("status_plan", F.concat("status", F.lit("/"), "plan"))
    .orderBy("end_date")
)

from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.year("end_date") == 2024)
    .select("id", "status", F.concat_ws("-", "status", "plan").alias("status_plan_key"))
)

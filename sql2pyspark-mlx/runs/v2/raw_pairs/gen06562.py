from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter((F.col("start_date") > "2023-07-23") | F.col("status").isin(["PAUSED", "EXPIRED", "ACTIVE"]))
    .select("id", "plan", "start_date", "status")
    .orderBy(F.col("plan").desc())
)

from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter((F.col("status") != "CANCELLED") & (F.col("end_date") > "2024-07-01") & F.col("status").isin(["PAUSED", "CANCELLED", "ACTIVE"]))
    .select("status", "customer_id", "end_date", F.trim("plan").alias("clean_plan"))
)

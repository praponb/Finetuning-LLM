from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter((F.col("plan") == "premium") & (F.col("status") == "EXPIRED") & F.col("status").isin(["EXPIRED", "CANCELLED", "PAUSED"]))
    .select("plan", "customer_id", F.last_day("end_date").alias("end_date_month_end"))
    .orderBy("customer_id")
)

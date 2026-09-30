from pyspark.sql import functions as F

filtered_subscriptions = (
    df_subscriptions
    .filter((F.col("end_date") >= "2024-02-01") & F.col("customer_id").isNotNull())
)

agg = (
    filtered_subscriptions
    .groupBy("plan", "status")
    .agg(F.sum("monthly_fee").alias("total_monthly_fee"))
)

result = (
    agg
    .filter(F.col("total_monthly_fee") > 590)
    .select("plan", "status")
    .orderBy(F.col("status").desc(), "plan")
    .limit(3)
)

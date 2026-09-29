from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.col("end_date") >= "2024-03-30")
    .select("id", "customer_id", "start_date", "monthly_fee")
    .orderBy(F.col("customer_id").desc(), F.col("start_date").desc())
)

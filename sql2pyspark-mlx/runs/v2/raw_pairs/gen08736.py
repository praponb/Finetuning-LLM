from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter((F.col("monthly_fee") > 240) & (F.col("start_date") < "2024-02-11"))
    .select("id", "customer_id", "plan", "end_date")
    .orderBy("end_date", F.col("customer_id").desc(), "id")
    .limit(20)
)

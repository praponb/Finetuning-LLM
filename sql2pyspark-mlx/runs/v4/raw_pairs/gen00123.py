from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.col("monthly_fee") > 80)
    .select("id", "monthly_fee", "customer_id")
    .orderBy("customer_id", F.col("monthly_fee").desc())
)

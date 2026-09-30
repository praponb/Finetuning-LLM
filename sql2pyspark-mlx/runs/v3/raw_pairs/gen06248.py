from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.col("monthly_fee") < 170)
    .select("customer_id", "plan", "status")
)

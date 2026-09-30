from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.col("monthly_fee") < 230)
    .select("id", "status", "plan", "customer_id")
)

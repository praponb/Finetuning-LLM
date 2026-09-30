from pyspark.sql import functions as F

result = (
    df_subscriptions
    .select("id", "monthly_fee", "status", "customer_id")
    .orderBy("customer_id")
)

from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.col("monthly_fee").between(70, 130))
    .select("plan", "monthly_fee", "customer_id")
)

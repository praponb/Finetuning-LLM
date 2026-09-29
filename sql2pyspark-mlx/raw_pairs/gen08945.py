from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.col("monthly_fee").between(120, 160))
    .select("id", "customer_id", "status", "end_date")
)

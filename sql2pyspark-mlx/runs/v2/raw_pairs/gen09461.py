from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.col("customer_id") == 12)
    .select("status", "start_date")
)

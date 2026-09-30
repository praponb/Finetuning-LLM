from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.col("start_date") <= "2024-04-01")
    .select("customer_id", "monthly_fee")
)

from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter((F.col("start_date") > "2023-05-01") & (F.col("customer_id") == 21))
    .select("start_date", "customer_id", "monthly_fee")
)

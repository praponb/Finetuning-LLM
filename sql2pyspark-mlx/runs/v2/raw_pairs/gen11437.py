from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.year("end_date") == 2023)
    .select("id", "customer_id", "status", "monthly_fee")
)

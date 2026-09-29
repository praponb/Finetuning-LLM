from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter((F.col("end_date") >= "2024-05-10") & (F.col("start_date") > "2025-06-01"))
    .select("plan", "customer_id", "end_date")
)

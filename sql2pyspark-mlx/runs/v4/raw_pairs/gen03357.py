from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.col("end_date") <= "2025-08-01")
    .select("id", "customer_id")
)

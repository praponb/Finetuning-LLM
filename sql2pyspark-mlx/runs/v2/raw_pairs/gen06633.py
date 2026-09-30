from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.col("start_date") < "2025-06-01")
    .select("status", "customer_id")
    .orderBy(F.col("customer_id").desc())
)

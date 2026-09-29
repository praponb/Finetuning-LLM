from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.col("customer_id") == 35)
    .select("id", "plan", "status")
    .orderBy(F.col("id").desc())
    .limit(50)
)

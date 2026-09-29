from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.col("customer_id") == 31)
    .select("plan")
    .distinct()
    .orderBy(F.col("plan").desc())
)

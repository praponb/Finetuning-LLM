from pyspark.sql import functions as F

result = (
    df_subscriptions
    .select("plan", "status")
    .distinct()
    .orderBy(F.col("status").desc())
)

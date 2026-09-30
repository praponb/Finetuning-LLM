from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.col("status") == "PAUSED")
    .select("status")
    .distinct()
    .orderBy("status")
)

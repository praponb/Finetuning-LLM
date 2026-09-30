from pyspark.sql import functions as F

result = (
    df_subscriptions
    .select("plan", "status")
    .distinct()
    .orderBy("status", "plan")
    .limit(50)
)

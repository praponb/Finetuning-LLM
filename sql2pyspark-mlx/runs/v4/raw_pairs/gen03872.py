from pyspark.sql import functions as F

result = (
    df_subscriptions
    .select("plan")
    .distinct()
    .orderBy("plan")
)

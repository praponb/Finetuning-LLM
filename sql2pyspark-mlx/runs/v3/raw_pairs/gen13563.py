from pyspark.sql import functions as F

result = (
    df_subscriptions
    .select("id", "status", "start_date")
    .orderBy("status", "start_date", "id")
    .limit(5)
)

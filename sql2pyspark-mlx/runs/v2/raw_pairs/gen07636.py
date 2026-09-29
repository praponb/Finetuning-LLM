from pyspark.sql import functions as F

result = (
    df_subscriptions
    .select("id", "status", "end_date")
    .orderBy("end_date")
)

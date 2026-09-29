from pyspark.sql import functions as F

result = (
    df_sessions
    .select("country")
    .union(df_subscriptions.select("status"))
    .distinct()
    .orderBy("country")
)

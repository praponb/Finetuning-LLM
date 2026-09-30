from pyspark.sql import functions as F

result = (
    df_shipments
    .select("status")
    .distinct()
    .crossJoin(df_subscriptions.select("plan").distinct())
    .orderBy("status", "plan")
)

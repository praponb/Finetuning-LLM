from pyspark.sql import functions as F

result = (
    df_shipments
    .select("status")
    .unionByName(df_subscriptions.select("status"))
    .distinct()
)

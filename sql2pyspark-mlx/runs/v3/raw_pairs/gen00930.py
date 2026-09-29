from pyspark.sql import functions as F

result = (
    df_shipments
    .select("status")
    .unionByName(df_subscriptions.filter(F.year("start_date") >= 2025).select("status"))
    .distinct()
    .orderBy("status")
)

from pyspark.sql import functions as F

result = (
    df_shipments
    .select("status")
    .intersect(df_subscriptions.filter(F.year("start_date") >= 2025).select("status"))
    .orderBy("status")
)

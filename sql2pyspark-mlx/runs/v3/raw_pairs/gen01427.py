from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("delivered_date") >= "2025-01-01")
    .select("status")
    .intersect(df_subscriptions.select("status"))
    .orderBy("status")
)

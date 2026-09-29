from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("status").isin(["COMPLETED", "PENDING", "CANCELLED"]))
    .select("status")
    .intersect(df_subscriptions.select("status"))
    .orderBy("status")
)

from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("order_id").isNotNull())
    .select("status")
    .subtract(df_subscriptions.select("status"))
    .orderBy(F.col("status").desc())
)

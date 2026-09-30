from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.col("order_id").isin([9, 12, 14, 28]))
    .select("status")
    .unionByName(df_subscriptions.select("status"))
    .distinct()
    .orderBy(F.col("status").desc())
)

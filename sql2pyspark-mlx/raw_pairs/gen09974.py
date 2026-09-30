from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.col("order_id").isNull() | (F.col("order_id") == 33))
    .groupBy(F.hour("paid_at").alias("paid_hour"))
    .agg(F.max("paid_at").alias("last_paid_at"))
    .orderBy("last_paid_at", "paid_hour")
    .limit(5)
)

from pyspark.sql import functions as F

recent_payments = df_payments.filter(F.col("amount") > 1270)

agg = (
    recent_payments
    .groupBy(F.hour("paid_at").alias("paid_hour"))
    .agg(
        F.round(F.avg("amount"), 2).alias("avg_amount"),
        F.max("amount").alias("max_amount"),
    )
)

result = (
    agg
    .select("paid_hour", "avg_amount", "max_amount")
    .orderBy(F.col("avg_amount").desc(), "paid_hour")
    .limit(10)
)

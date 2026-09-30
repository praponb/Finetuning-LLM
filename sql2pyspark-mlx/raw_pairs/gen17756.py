from pyspark.sql import functions as F

recent_payments = (
    df_payments
    .filter((F.col("amount") > 1160) & (F.col("method") == "bank_transfer") & (F.col("amount") <= 1370))
    .withColumn("paid_at_dayofweek", F.dayofweek("paid_at"))
)

payments_stats = (
    recent_payments
    .groupBy("status")
    .agg(
        F.min("paid_at_dayofweek").alias("min_paid_at_dayofweek"),
        (F.max("paid_at_dayofweek") - F.min("paid_at_dayofweek")).alias("paid_at_dayofweek_range"),
    )
)

result = (
    payments_stats
    .filter(F.col("paid_at_dayofweek_range") >= 55)
    .orderBy(F.col("status").desc(), F.col("min_paid_at_dayofweek").desc())
)

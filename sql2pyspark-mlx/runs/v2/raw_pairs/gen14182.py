from pyspark.sql import functions as F

filtered_payments = (
    df_payments
    .filter(F.col("order_id").isin([1, 27, 28, 37]) & ~F.col("status").isin(["FAILED", "REFUNDED"]))
)

agg = (
    filtered_payments
    .groupBy(F.hour("paid_at").alias("paid_hour"))
    .agg(
        F.round(((100 * F.sum(F.when(F.col("status") == "FAILED", 1).otherwise(0))) / F.count("*")), 1).alias("pct_failed"),
        F.max("paid_at").alias("latest_paid_at"),
    )
)

result = agg.orderBy(F.col("paid_hour").desc())

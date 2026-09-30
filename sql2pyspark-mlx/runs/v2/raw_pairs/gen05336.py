from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.col("order_id").isin([23, 25]))
    .groupBy("status", F.to_date("paid_at").alias("paid_day"))
    .agg(
        F.sum(F.when(F.col("method") == "cash", 1).otherwise(0)).alias("cash_count"),
        F.round(F.avg("amount"), 2).alias("avg_amount"),
    )
    .filter(F.col("avg_amount") < 31)
    .select("status", "paid_day", "cash_count", "avg_amount")
    .orderBy("paid_day")
)

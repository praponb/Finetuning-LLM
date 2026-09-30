from pyspark.sql import functions as F

base = (
    df_payments
    .filter(F.to_date("paid_at") >= "2024-04-28")
    .withColumn("status_group", F.when(F.col("status").isin(["SUCCESS", "REFUNDED"]), "Group A").otherwise("Other"))
)

result = (
    base
    .withColumn("activity_month", F.date_trunc("MONTH", "paid_at"))
    .withColumn("year_start", F.date_trunc("YEAR", "paid_at"))
    .rollup("activity_month", "order_id", "year_start")
    .agg(
        F.sum(F.when(F.col("method") == "promptpay", F.col("amount")).otherwise(0)).alias("promptpay_amount"),
        F.avg(F.when(F.col("method") == "bank_transfer", F.col("amount"))).alias("bank_transfer_avg_amount"),
        F.count_if(F.col("status") == "FAILED").alias("failed_events"),
        F.grouping("activity_month").alias("gid"),
    )
    .filter(F.col("failed_events") > 1)
    .orderBy(F.col("order_id").desc())
)

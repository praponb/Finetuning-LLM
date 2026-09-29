from pyspark.sql import functions as F

valid_payments = (
    df_payments
    .filter(F.col("status") != "SUCCESS")
    .withColumn("amount_int", F.col("amount").cast("int"))
)

payments_stats = (
    valid_payments
    .groupBy(F.month("paid_at").alias("paid_month"))
    .agg(
        F.count(F.when(F.col("status") == "SUCCESS", 1)).alias("success_count"),
        F.avg("amount").alias("avg_amount"),
    )
)

result = payments_stats.filter(F.col("success_count") < 9)

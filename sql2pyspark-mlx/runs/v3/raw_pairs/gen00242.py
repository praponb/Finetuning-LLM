from pyspark.sql import functions as F

result = (
    df_payments
    .groupBy(F.month("paid_at").alias("paid_month"))
    .agg(
        F.sum(F.when(F.col("status") == "REFUNDED", F.col("amount")).otherwise(0)).alias("refunded_amount"),
    )
    .filter(F.col("refunded_amount") < 43)
    .select("paid_month", "refunded_amount")
    .orderBy("paid_month")
)

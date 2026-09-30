from pyspark.sql import functions as F

src = df_payments.filter(F.col("order_id") == 29)

agg = (
    src
    .groupBy("status", F.date_format("paid_at", "yyyy-MM").alias("paid_month"))
    .agg(
        F.sum(F.when(F.col("status") == "SUCCESS", F.col("amount")).otherwise(0)).alias("success_amount"),
        F.avg("amount").alias("avg_amount"),
    )
)

result = (
    agg
    .select("status", "paid_month", "success_amount", "avg_amount")
    .orderBy("success_amount")
)

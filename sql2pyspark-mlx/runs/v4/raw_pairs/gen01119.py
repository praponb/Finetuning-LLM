from pyspark.sql import functions as F

result = (
    df_payments
    .groupBy(F.month("paid_at").alias("paid_month"), "status")
    .agg(F.sum("amount").alias("total_amount"))
    .orderBy("status")
)

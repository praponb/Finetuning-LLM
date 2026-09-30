from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.col("paid_at") < "2024-03-01")
    .groupBy(F.date_format("paid_at", "yyyy-MM").alias("paid_month"))
    .agg(
        F.count(F.when(F.col("status") == "SUCCESS", 1)).alias("success_count"),
        (F.max("amount") - F.min("amount")).alias("amount_range"),
    )
)

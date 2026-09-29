from pyspark.sql import functions as F

src = df_payments.filter((F.col("paid_at") < "2025-08-06") & (F.col("method") == "cash"))

amount_bucket_summary = (
    src
    .groupBy(
        F.when(F.col("amount") >= 1700, "high").when(F.col("amount") >= 370, "mid").otherwise("low").alias("amount_bucket"),
    )
    .agg(F.max("paid_at").alias("last_paid_at"))
)

result = (
    amount_bucket_summary
    .select("amount_bucket", "last_paid_at")
    .orderBy("amount_bucket")
)

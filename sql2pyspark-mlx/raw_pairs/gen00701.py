from pyspark.sql import functions as F

src = (
    df_payments
    .filter(~F.col("status").isin(["SUCCESS", "FAILED"]) & (F.col("method") != "bank_transfer"))
)

amount_bucket_summary = (
    src
    .groupBy(
        F.when(F.col("amount") >= 1790, "high").when(F.col("amount") >= 710, "mid").otherwise("low").alias("amount_bucket"),
    )
    .agg((F.max("amount") - F.min("amount")).alias("amount_range"))
)

result = (
    amount_bucket_summary
    .filter(F.col("amount_range") > 52)
    .orderBy("amount_bucket", F.col("amount_range").desc())
)

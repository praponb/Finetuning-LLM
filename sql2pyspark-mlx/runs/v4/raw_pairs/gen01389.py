from pyspark.sql import functions as F

result = (
    df_payments
    .groupBy(
        F.when(F.col("status").isin(["FAILED", "SUCCESS"]), "core").otherwise("other").alias("status_group"),
        "status",
    )
    .agg(
        F.min("paid_at").alias("min_paid_at"),
        F.min("amount").alias("min_amount"),
    )
    .filter(F.col("min_amount") >= 820)
    .orderBy(F.col("min_amount").desc(), "min_paid_at")
)

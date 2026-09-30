from pyspark.sql import functions as F

recent_payments = (
    df_payments
    .filter(F.hour("paid_at").between(11, 18))
    .withColumn("status_group", F.when(F.col("status").isin(["SUCCESS", "FAILED"]), "Core").otherwise("Other"))
)

method_summary = (
    recent_payments
    .groupBy("method")
    .agg(
        F.count("amount").alias("amount_count"),
        F.sum(F.when(F.col("status") == "REFUNDED", 1).otherwise(0)).alias("refunded_count"),
    )
)

result = (
    method_summary
    .filter(F.col("refunded_count") >= 28)
    .select("method", "amount_count", "refunded_count")
    .orderBy("refunded_count")
)

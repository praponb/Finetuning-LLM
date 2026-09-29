from pyspark.sql import functions as F

base = (
    df_payments
    .filter(F.col("paid_at") < "2025-04-28")
    .withColumn("status_group", F.when(F.col("status").isin(["REFUNDED", "FAILED"]), "Tier 1").otherwise("Other"))
)

summary = (
    base
    .groupBy("method")
    .agg(
        F.max("amount").alias("max_amount"),
        F.round(F.avg("amount"), 2).alias("avg_amount"),
    )
)

result = summary

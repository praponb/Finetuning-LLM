from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.hour("paid_at").between(9, 14))
    .groupBy(
        "method",
        F.when(F.col("amount") >= 1490, "high").when(F.col("amount") >= 290, "mid").otherwise("low").alias("amount_bucket"),
        F.when(F.col("status").isin(["FAILED", "SUCCESS"]), "tier_1").otherwise("other").alias("status_group"),
    )
    .agg(
        F.round(F.stddev("amount"), 2).alias("stddev_amount"),
        F.avg("amount").alias("avg_amount"),
    )
)

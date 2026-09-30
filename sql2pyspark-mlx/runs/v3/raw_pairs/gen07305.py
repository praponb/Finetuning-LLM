from pyspark.sql import functions as F

src = (
    df_payments
    .filter(F.col("order_id").isin([1, 4]) & F.col("amount").between(760, 1160))
)

payments_stats = (
    src
    .groupBy(
        F.when(F.col("method").isin(["cash", "bank_transfer"]), "tier_1").otherwise("other").alias("method_group"),
    )
    .agg(
        F.round(((100 * F.sum(F.when(F.col("status") == "FAILED", 1).otherwise(0))) / F.count("*")), 1).alias("pct_failed"),
    )
)

result = payments_stats.select("method_group", "pct_failed")

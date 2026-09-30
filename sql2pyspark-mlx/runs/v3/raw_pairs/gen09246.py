from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(~F.col("is_flagged") & (F.col("amount") < 1900))
    .groupBy(
        F.when(F.col("amount") >= 3300, "high").when(F.col("amount") >= 1900, "mid").otherwise("low").alias("amount_bucket"),
    )
    .agg(
        F.round(((100 * F.sum(F.when(F.col("merchant") == "Shell", 1).otherwise(0))) / F.count("*")), 1).alias("pct_shell"),
        F.round(F.avg("amount"), 2).alias("avg_amount"),
    )
    .orderBy(F.col("pct_shell").desc(), "amount_bucket")
    .limit(20)
)

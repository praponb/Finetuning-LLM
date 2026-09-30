from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(~F.col("is_flagged"))
    .groupBy(
        F.when(F.col("amount") >= 3300, "high").when(F.col("amount") >= 1000, "mid").otherwise("low").alias("amount_bucket"),
        "account_id",
        "txn_type",
    )
    .agg(
        F.round(((100 * F.sum(F.when(F.col("merchant") == "Grab", 1).otherwise(0))) / F.count("*")), 1).alias("pct_grab"),
    )
)

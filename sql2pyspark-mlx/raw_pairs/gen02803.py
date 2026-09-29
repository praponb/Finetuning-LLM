from pyspark.sql import functions as F

result = (
    df_accounts
    .rollup("customer_id")
    .agg(
        F.round(F.sum("balance") / F.nullif(F.count(F.when(F.col("account_type") == "CREDIT", 1)), F.lit(0)), 2).alias("balance_per_credit"),
        F.round(F.avg("balance"), 2).alias("avg_balance"),
        F.countDistinct("id").alias("distinct_ids"),
        F.percentile_approx("balance", [0.25, 0.5, 0.75]).alias("balance_quartiles"),
        F.grouping_id().alias("grouping_level"),
    )
    .orderBy(F.col("balance_per_credit").desc())
)

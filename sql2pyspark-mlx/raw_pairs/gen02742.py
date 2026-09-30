from pyspark.sql import functions as F

result = (
    df_accounts
    .groupBy(
        F.when(F.col("balance") >= 80000, "PREMIUM").otherwise("STANDARD").alias("balance_tier"),
    )
    .agg(
        F.percentile_approx("balance", [0.25, 0.5, 0.75]).alias("balance_quartiles"),
        F.sort_array(F.collect_set("status")).alias("status_list"),
        F.max(F.when(F.col("account_type") == "LOAN", F.col("balance"))).alias("loan_max_balance"),
    )
)

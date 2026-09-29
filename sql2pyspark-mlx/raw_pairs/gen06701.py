from pyspark.sql import functions as F

result = (
    df_accounts
    .groupBy(
        F.when(F.col("balance") >= 81000, "HIGH").otherwise("LOW").alias("balance_tier"),
        "account_type",
    )
    .agg(
        F.sort_array(F.collect_set("status")).alias("status_list"),
        F.count(F.when(F.col("status") == "FROZEN", 1)).alias("frozen_count"),
        F.count(F.when(F.col("account_type") == "LOAN", 1)).alias("loan_count"),
        F.percentile_approx("balance", [0.25, 0.5, 0.75]).alias("balance_quartiles"),
    )
    .filter(F.col("frozen_count") > 1)
    .orderBy(F.col("balance_tier").desc_nulls_last())
)

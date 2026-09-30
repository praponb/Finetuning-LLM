from pyspark.sql import functions as F

result = (
    df_accounts
    .withColumn("balance_tier", F.when(F.col("balance") >= 65000, "PREMIUM").otherwise("STANDARD"))
    .groupingSets(
        [["balance_tier", "account_type"], ["balance_tier"]],
        "balance_tier",
        "account_type",
    )
    .agg(
        F.round(F.avg("balance"), 2).alias("avg_balance"),
        F.sort_array(F.collect_set("account_type")).alias("account_type_list"),
        F.round(F.avg("balance"), 2).alias("avg_balance_2"),
        F.count("*").alias("num_rows"),
    )
)

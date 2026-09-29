from pyspark.sql import functions as F

base = (
    df_accounts
    .filter(F.col("balance") > 56000)
    .withColumn("account_type_status_key", F.concat_ws(",", "account_type", "status"))
    .withColumn("status_clean", F.coalesce("status", F.lit("NONE")))
)

not_flagged = (
    base
    .alias("s")
    .join(df_transactions.alias("x"), (F.col("x.account_id") == F.col("s.id")) & (F.col("x.amount") > 1600), "left_anti")
)

cleaned = (
    not_flagged
    .filter(F.col("customer_id").isNotNull())
    .select(
        "id",
        "customer_id",
        "account_type",
        "balance",
        "status",
        "account_type_status_key",
        "status_clean",
        F.date_add("opened_date", 82).alias("opened_date_plus_82d"),
        F.coalesce("status", F.lit("N/A")).alias("resolved_status"),
    )
)

not_flagged_accounts = (
    cleaned
    .alias("s")
    .join(df_transactions.alias("ch"), (F.col("ch.account_id") == F.col("s.id")) & (F.col("ch.is_flagged")), "left_anti")
)

grouped = (
    not_flagged_accounts
    .groupBy(
        F.when(F.col("balance") >= 88000, "LARGE").otherwise("SMALL").alias("balance_tier"),
    )
    .agg(
        F.percentile_approx("balance", [0.25, 0.5, 0.75]).alias("balance_quartiles"),
        F.percentile_approx("balance", 0.9).alias("balance_p90"),
        F.sum("balance").alias("total_balance"),
        F.sort_array(F.collect_set("account_type")).alias("account_type_list"),
        F.count_if(F.col("account_type") == "SAVINGS").alias("savings_events"),
    )
    .filter(F.col("total_balance") < 0)
)

result = (
    grouped
    .select(
        "balance_tier",
        "balance_quartiles",
        "balance_p90",
        "total_balance",
        "account_type_list",
        "savings_events",
    )
    .orderBy(F.col("balance_p90").desc_nulls_first())
)

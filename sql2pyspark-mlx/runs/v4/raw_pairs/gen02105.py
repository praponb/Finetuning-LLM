from pyspark.sql import functions as F

cleaned = (
    df_accounts
    .filter(F.col("status").isNotNull() & (F.col("account_type") == "CHECKING"))
    .select(
        "id",
        "customer_id",
        "account_type",
        "balance",
        "status",
        F.substring("status", 1, 4).alias("status_prefix"),
        F.when(F.col("account_type") == "CREDIT", 1).otherwise(0).alias("is_credit"),
    )
)

base = (
    cleaned
    .filter(F.col("account_type").isin(["LOAN", "CHECKING", "CREDIT"]))
    .select(
        "id",
        "customer_id",
        "balance",
        "status",
        "status_prefix",
        "is_credit",
        F.substring("status", 1, 2).alias("status_prefix_2"),
    )
)

not_flagged = (
    base
    .alias("s")
    .join(df_transactions.alias("ch"), (F.col("ch.account_id") == F.col("s.id")) & (F.col("ch.is_flagged")), "left_anti")
)

result = (
    not_flagged
    .groupBy("status")
    .agg(
        F.percentile_approx("is_credit", 0.95).alias("is_credit_p95"),
        F.count_if(F.col("status") == "FROZEN").alias("frozen_events"),
        F.percentile_approx("balance", 0.9).alias("balance_p90"),
    )
)

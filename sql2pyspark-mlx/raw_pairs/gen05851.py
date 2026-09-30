from pyspark.sql import functions as F

base = (
    df_accounts
    .filter((F.col("opened_date") >= F.lit("2023-03-09").cast("date")) & (F.col("balance") > 32000))
    .select(
        "*",
        (F.datediff(F.current_date(), "opened_date") / 365.25).alias("years_since_opened_date"),
        F.coalesce("status", F.lit("N/A")).alias("resolved_status"),
    )
)

kept = (
    base
    .alias("s")
    .join(df_transactions.alias("x"), (F.col("x.account_id") == F.col("s.id")) & (F.col("x.txn_time") < F.col("s.opened_date")), "left_anti")
)

enriched = (
    kept
    .alias("s")
    .join(df_customers.alias("c"), F.col("s.customer_id") == F.col("c.id"), "left")
    .select(
        "s.id",
        "s.customer_id",
        "s.account_type",
        "s.balance",
        "s.status",
        "s.years_since_opened_date",
        F.col("c.country"),
    )
)

not_flagged = (
    enriched
    .alias("s")
    .join(df_transactions.alias("c"), (F.col("c.account_id") == F.col("s.id")) & (F.col("c.txn_time") < "2023-06-04"), "left_anti")
)

result = (
    not_flagged
    .groupBy(
        F.when(F.col("years_since_opened_date") >= 76, "LARGE").otherwise("SMALL").alias("years_since_opened_date_tier"),
    )
    .agg(
        F.round(F.avg("balance"), 2).alias("avg_balance"),
        F.count(F.when(F.col("account_type") == "CHECKING", 1)).alias("checking_count"),
        F.sort_array(F.collect_set("status")).alias("status_list"),
        F.percentile_approx("balance", [0.25, 0.5, 0.75]).alias("balance_quartiles"),
        F.count(F.when(F.col("status") == "FROZEN", 1)).alias("frozen_count"),
    )
    .orderBy(F.col("avg_balance").desc())
)

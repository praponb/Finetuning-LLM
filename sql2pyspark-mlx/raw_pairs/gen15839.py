from pyspark.sql import functions as F

base = (
    df_accounts
    .filter((F.col("opened_date") >= F.to_date(F.lit("2023-05-29"))) & (F.col("balance") < 47000))
    .select(
        "id",
        "customer_id",
        "balance",
        "opened_date",
        "status",
        F.date_format("opened_date", "MMM yyyy").alias("opened_date_label"),
        F.coalesce("status", F.lit("OTHER")).alias("status_clean"),
    )
)

remaining_keys = (
    base
    .select("id")
    .exceptAll(
        df_accounts
        .filter(F.col("status") == "FROZEN")
        .select("id")
    )
)

kept_rows = (
    base
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

kept = (
    kept_rows
    .alias("s")
    .join(df_transactions.alias("c"), (F.col("c.account_id") == F.col("s.id")) & (F.col("c.amount") >= 1900), "left_semi")
)

kept_keys = (
    kept
    .select("customer_id", "status")
    .subtract(
        df_accounts
        .filter(F.col("account_type") == "CHECKING")
        .select("customer_id", "status")
    )
)

kept_rows_accounts = (
    kept
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.customer_id") == F.col("k.customer_id")) & (F.col("s.status") == F.col("k.status")), "inner")
    .select("s.*")
)

base_accounts = (
    kept_rows_accounts
    .filter((F.col("opened_date") >= F.lit("2023-03-23").cast("date")) & F.col("status").isin(["CLOSED", "FROZEN"]))
    .select(
        "id",
        "balance",
        "opened_date",
        "opened_date_label",
        "status_clean",
        F.date_format("opened_date", "yyyy-MM").alias("opened_date_label_2"),
        F.last_day("opened_date").alias("opened_date_month_end"),
    )
)

kept_accounts = (
    base_accounts
    .alias("s")
    .join(df_transactions.alias("c"), (F.col("c.account_id") == F.col("s.id")) & (F.col("c.account_id").isin([10, 18, 24])), "left_anti")
)

daily = (
    kept_accounts
    .groupBy(F.date_trunc("MONTH", "opened_date_month_end").alias("activity_month"))
    .agg(
        F.round(F.avg("balance"), 2).alias("avg_balance"),
        F.percentile_approx("balance", [0.25, 0.5, 0.75]).alias("balance_quartiles"),
        F.count("*").alias("row_count"),
    )
    .filter(F.col("avg_balance") > 0)
)

result = (
    daily
    .select("activity_month", "balance_quartiles")
    .orderBy(F.col("activity_month").desc_nulls_last())
)

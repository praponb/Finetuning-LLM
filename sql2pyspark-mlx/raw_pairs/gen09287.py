from pyspark.sql import functions as F

remaining_keys = (
    df_accounts
    .select("id", "customer_id")
    .subtract(
        df_accounts
        .filter(F.col("account_type").isin(["CREDIT", "SAVINGS"]))
        .select("id", "customer_id")
    )
)

reconciled = (
    df_accounts
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.customer_id") == F.col("k.customer_id")), "inner")
    .select("s.*")
)

eligible = (
    reconciled
    .alias("s")
    .join(df_transactions.alias("c"), (F.col("c.account_id") == F.col("s.id")) & (F.col("c.txn_time") < "2025-07-01"), "left_semi")
)

matched_keys = (
    eligible
    .select("id")
    .subtract(
        df_accounts
        .filter(~F.col("status").isin(["FROZEN", "CLOSED"]))
        .select("id")
    )
)

kept_rows = (
    eligible
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

enriched = (
    kept_rows
    .alias("e")
    .join(df_customers.alias("p"), F.col("e.customer_id").eqNullSafe(F.col("p.id")), "left")
    .select("e.*", F.col("p.segment"), F.col("p.email"), F.col("p.age"))
)

kept_keys = (
    enriched
    .select("id", "customer_id")
    .subtract(
        df_accounts
        .filter(F.col("customer_id").isNull())
        .select("id", "customer_id")
    )
)

reconciled_accounts = (
    enriched
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.customer_id") == F.col("k.customer_id")), "inner")
    .select("s.*")
)

kept = (
    reconciled_accounts
    .alias("s")
    .join(df_transactions.alias("x"), (F.col("x.account_id") == F.col("s.id")) & (F.col("x.txn_time") < F.col("s.opened_date")), "left_semi")
)

result = (
    kept
    .alias("src")
    .join(df_customers.alias("d"), F.col("src.customer_id") == F.col("d.id"), "left")
    .rollup(
        F.col("d.city").alias("city"),
        F.col("src.segment").alias("account_segment"),
    )
    .agg(
        F.count("*").alias("num_rows"),
        F.min(F.col("src.balance")).alias("min_balance"),
        F.grouping_id().alias("level_id"),
    )
)

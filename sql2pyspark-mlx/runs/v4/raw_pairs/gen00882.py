from pyspark.sql import functions as F

kept_keys = (
    df_accounts
    .select("id", "status")
    .exceptAll(
        df_accounts
        .filter(F.col("account_type").isin(["SAVINGS", "CREDIT", "CHECKING"]))
        .select("id", "status")
    )
)

reconciled = (
    df_accounts
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.status") == F.col("k.status")), "inner")
    .select("s.*")
)

kept = (
    reconciled
    .alias("s")
    .join(df_transactions.alias("c"), (F.col("c.account_id") == F.col("s.id")) & (F.col("c.txn_time") <= F.col("s.opened_date")), "left_semi")
)

with_dims = (
    kept
    .alias("src")
    .join(df_customers.alias("p"), F.col("src.customer_id") == F.col("p.id"), "left")
    .select("src.*", F.col("p.credit_limit"))
)

cleaned = (
    with_dims
    .filter((F.col("opened_date") > F.lit("2024-01-25").cast("date")) & F.col("status").isNotNull())
    .withColumn("account_type_lower", F.lower("account_type"))
    .withColumn("status_filled", F.coalesce("status", F.lit("NONE")))
)

summary = (
    cleaned
    .groupBy("customer_id")
    .agg(
        F.sum(F.when(F.col("status") == "FROZEN", F.col("balance")).otherwise(0)).alias("frozen_balance"),
        F.sort_array(F.collect_set("status")).alias("status_list"),
    )
    .filter(F.col("frozen_balance") > 0)
)

result = (
    summary
    .select("customer_id", "frozen_balance", "status_list")
    .orderBy(F.col("customer_id").desc_nulls_first())
    .limit(10)
)

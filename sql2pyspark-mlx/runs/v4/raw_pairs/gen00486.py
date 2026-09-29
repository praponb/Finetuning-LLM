from pyspark.sql import functions as F

base = (
    df_accounts
    .filter(F.col("customer_id").isNotNull() & (F.col("status") == "OPEN"))
    .select(
        "*",
        F.concat("account_type", F.lit("/"), "status").alias("account_type_status"),
        F.date_format("opened_date", "MMM yyyy").alias("opened_date_label"),
        F.coalesce("status", F.lit("UNKNOWN")).alias("status_filled"),
    )
)

remaining_keys = (
    base
    .select("customer_id", "account_type")
    .intersect(
        df_accounts
        .filter(F.col("balance") <= 77000)
        .select("customer_id", "account_type")
    )
)

kept_rows = (
    base
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.customer_id") == F.col("k.customer_id")) & (F.col("s.account_type") == F.col("k.account_type")), "inner")
    .select("s.*")
)

not_flagged = (
    kept_rows
    .alias("s")
    .join(df_transactions.alias("x"), (F.col("x.account_id") == F.col("s.id")) & (F.col("x.merchant").isin(["Shell", "Lazada"])), "left_anti")
)

filtered = (
    not_flagged
    .filter((F.col("opened_date") >= F.to_date(F.lit("2023-06-19"))) & (F.year("opened_date") == 2023))
    .select(
        "id",
        "customer_id",
        "account_type",
        "balance",
        "account_type_status",
        "status_filled",
        F.initcap(F.trim("account_type")).alias("account_type_title"),
    )
)

eligible = (
    filtered
    .alias("s")
    .join(df_transactions.alias("c"), (F.col("c.account_id") == F.col("s.id")) & (F.col("c.account_id").isin([1, 4, 10, 26])), "left_anti")
)

result = (
    eligible
    .groupBy("account_type", "customer_id")
    .agg(
        F.sum(F.when(F.col("account_type") == "SAVINGS", F.col("balance"))).alias("savings_total_balance"),
        F.percentile_approx("balance", [0.25, 0.5, 0.75]).alias("balance_quartiles"),
        F.count(F.when(F.col("account_type") == "CREDIT", 1)).alias("credit_count"),
        F.min("balance").alias("min_balance"),
        F.percentile_approx("balance", [0.25, 0.5, 0.75]).alias("balance_quartiles_2"),
    )
    .filter(F.col("credit_count") > 1)
    .orderBy(F.col("customer_id").desc())
)

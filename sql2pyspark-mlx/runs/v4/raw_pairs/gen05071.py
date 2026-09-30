from pyspark.sql import functions as F

cleaned = (
    df_accounts
    .filter((F.col("opened_date") >= F.lit("2023-09-26").cast("date")) & (F.col("account_type") == "CREDIT"))
    .select(
        "id",
        "customer_id",
        "account_type",
        "balance",
        "opened_date",
        "status",
        F.date_format("opened_date", "yyyy-MM").alias("opened_date_label"),
        F.date_format("opened_date", "yyyy-MM").alias("opened_date_label_2"),
        F.coalesce("status", F.lit("N/A")).alias("resolved_status"),
    )
)

with_dims = (
    cleaned
    .alias("a")
    .join(df_customers.alias("b"), F.col("a.customer_id") == F.col("b.id"), "left")
    .select("a.*", F.col("b.segment"))
)

kept_keys = (
    with_dims
    .select("account_type", "status")
    .exceptAll(
        df_accounts
        .filter(F.col("customer_id").isNotNull())
        .select("account_type", "status")
    )
)

verified = (
    with_dims
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.account_type") == F.col("k.account_type")) & (F.col("s.status") == F.col("k.status")), "inner")
    .select("s.*")
)

eligible = (
    verified
    .alias("s")
    .join(df_transactions.alias("ch"), (F.col("ch.account_id") == F.col("s.id")) & (F.col("ch.txn_time") >= "2024-05-05"), "left_anti")
)

result = (
    eligible
    .select(
        "account_type",
        "balance",
        "opened_date",
        "status",
        "opened_date_label",
        "opened_date_label_2",
    )
    .orderBy(F.col("status").desc())
)

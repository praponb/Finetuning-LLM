from pyspark.sql import functions as F

kept = (
    df_accounts
    .alias("s")
    .join(df_transactions.alias("c"), (F.col("c.account_id") == F.col("s.id")) & (F.col("c.txn_type") == "DEBIT"), "left_anti")
)

not_flagged = (
    kept
    .alias("s")
    .join(df_transactions.alias("c"), (F.col("c.account_id") == F.col("s.id")) & (F.col("c.txn_time") >= F.col("s.opened_date")), "left_semi")
)

kept_accounts = (
    not_flagged
    .alias("s")
    .join(df_transactions.alias("x"), (F.col("x.account_id") == F.col("s.id")) & (F.col("x.txn_time") >= F.col("s.opened_date")), "left_anti")
)

eligible = (
    kept_accounts
    .alias("s")
    .join(df_transactions.alias("ch"), (F.col("ch.account_id") == F.col("s.id")) & (F.col("ch.amount") <= 1800), "left_anti")
)

result = (
    eligible
    .select(
        "id",
        "account_type",
        "balance",
        "opened_date",
        F.when(F.col("account_type").rlike("^(SAVINGS|LOAN)$"), "OTHER_ZONE").otherwise("CORE").alias("account_type_zone"),
    )
    .orderBy(F.col("opened_date").asc_nulls_first())
)

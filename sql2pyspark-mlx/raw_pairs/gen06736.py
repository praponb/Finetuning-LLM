from pyspark.sql import functions as F

kept = (
    df_accounts
    .alias("s")
    .join(df_transactions.alias("ch"), (F.col("ch.account_id") == F.col("s.id")) & (F.col("ch.txn_time") <= F.col("s.opened_date")), "left_semi")
)

result = (
    kept
    .select(
        "id",
        "customer_id",
        "account_type",
        "balance",
        "opened_date",
        "status",
        F.when(F.col("account_type").rlike("^(CHECKING|CREDIT)$"), "PRIORITY").when(F.col("account_type").rlike("^(SAVINGS|LOAN)$"), "CORE").otherwise("GROUP_A").alias("account_type_zone"),
    )
)

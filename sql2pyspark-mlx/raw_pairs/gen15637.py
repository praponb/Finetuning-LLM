from pyspark.sql import functions as F

valid = (
    df_accounts
    .filter(F.col("status").isNotNull() & (F.col("status") != "OPEN"))
    .select(
        "id",
        "customer_id",
        "account_type",
        "balance",
        "opened_date",
        "status",
        F.substring("account_type", 1, 3).alias("account_type_prefix"),
    )
)

result = (
    valid
    .alias("s")
    .join(df_transactions.alias("ch"), (F.col("ch.account_id") == F.col("s.id")) & (F.col("ch.txn_time") <= F.col("s.opened_date")), "left_anti")
)

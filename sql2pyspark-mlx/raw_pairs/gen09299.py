from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.txn_time").desc(), F.col("c.id").desc())

valid = (
    df_accounts
    .filter(F.col("status").isNotNull() & F.col("status").isNotNull())
    .withColumn("account_type_status", F.concat("account_type", F.lit("/"), "status"))
    .withColumn("status_lower", F.lower("status"))
)

with_latest = (
    valid
    .alias("s")
    .join(df_transactions.alias("c"), (F.col("c.account_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.customer_id",
        "s.account_type",
        "s.balance",
        "s.status_lower",
        F.col("c.amount").alias("transaction_amount"),
        F.col("c.txn_type"),
    )
)

kept = (
    with_latest
    .alias("s")
    .join(df_transactions.alias("ch"), (F.col("ch.account_id") == F.col("s.id")) & (~F.col("ch.is_flagged")), "left_anti")
)

valid_accounts = (
    kept
    .filter(F.col("transaction_amount") > 3400)
    .select(
        "*",
        F.abs("balance").alias("abs_balance"),
        F.coalesce("txn_type", F.lit("DIRECT")).alias("resolved_txn_type"),
    )
)

matched_keys = (
    valid_accounts
    .select("customer_id")
    .subtract(
        df_accounts
        .filter(F.col("account_type") != "SAVINGS")
        .select("customer_id")
    )
)

kept_rows = (
    valid_accounts
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.customer_id") == F.col("k.customer_id")), "inner")
    .select("s.*")
)

result = (
    kept_rows
    .select("account_type", "abs_balance", "balance", "transaction_amount")
    .unpivot(
        ids=["account_type"],
        values=["abs_balance", "balance", "transaction_amount"],
        variableColumnName="measure",
        valueColumnName="value",
    )
    .filter(F.col("value").isNotNull())
)

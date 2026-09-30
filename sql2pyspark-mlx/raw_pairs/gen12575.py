from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy("txn_time_period", "id")
w2 = Window.partitionBy("status").orderBy(F.col("txn_time_period").cast("bigint")).rangeBetween(-2592000, Window.currentRow)
amount_by_account_id = df_transactions.groupBy("account_id").agg(F.sum("amount").alias("sum_transaction_amount"))

filtered = (
    df_transactions
    .filter((F.col("merchant").eqNullSafe(F.lit("7-Eleven"))) & (F.col("txn_time") >= F.lit("2023-10-18").cast("date")))
    .select(
        "id",
        "account_id",
        "txn_type",
        "amount",
        "txn_time",
        "merchant",
        F.date_trunc("year", "txn_time").alias("txn_time_period"),
    )
)

with_dims = (
    filtered
    .alias("a")
    .join(df_accounts.alias("p"), F.col("a.account_id") == F.col("p.id"), "left")
    .filter(F.col("p.opened_date") < "2025-01-01")
    .select(
        "a.id",
        "a.account_id",
        "a.txn_time",
        "a.merchant",
        "a.txn_time_period",
        F.col("p.balance"),
        F.col("p.opened_date"),
        F.col("p.account_type"),
    )
)

with_dims_transactions = (
    with_dims
    .alias("a")
    .join(df_accounts.alias("p"), F.col("a.account_id") == F.col("p.id"), "left")
    .select("a.*", F.col("p.status"))
)

metrics = (
    with_dims_transactions
    .select(
        "*",
        F.lead("balance").over(w).alias("next_balance"),
        F.lag("balance").over(w).alias("prev_balance"),
        F.row_number().over(w).alias("seq_no"),
        F.max("balance").over(w2).alias("balance_max_30d"),
    )
)

result = (
    metrics
    .join(amount_by_account_id, on="account_id", how="left")
    .select(
        "id",
        "account_id",
        "txn_time",
        "balance",
        "opened_date",
        "account_type",
        "prev_balance",
        "balance_max_30d",
        F.when(F.col("account_type").rlike("^(CREDIT|LOAN)$"), "OTHER_ZONE").when(F.col("account_type").rlike("^(SAVINGS|CHECKING)$"), "EXTENDED").otherwise("GROUP_A").alias("account_type_zone"),
        F.col("sum_transaction_amount"),
    )
    .orderBy(F.col("account_type").desc_nulls_first(), "opened_date")
)

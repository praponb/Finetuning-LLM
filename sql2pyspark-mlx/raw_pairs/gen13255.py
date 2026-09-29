from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status")
w2 = Window.partitionBy("status").orderBy("opened_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_ordered = Window.partitionBy("status", "account_type").orderBy(F.col("account_type_length").desc_nulls_last())
w4 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.txn_time").desc(), F.col("c.id").desc())

base = (
    df_accounts
    .filter(F.col("status").isNotNull())
    .select(
        "id",
        "account_type",
        "balance",
        "opened_date",
        "status",
        F.length("account_type").alias("account_type_length"),
    )
)

kept = (
    base
    .alias("s")
    .join(df_transactions.alias("ch"), (F.col("ch.account_id") == F.col("s.id")) & (F.col("ch.account_id") == 23), "left_semi")
)

not_flagged = (
    kept
    .alias("s")
    .join(df_transactions.alias("ch"), (F.col("ch.account_id") == F.col("s.id")) & (F.col("ch.txn_time") < F.col("s.opened_date")), "left_anti")
)

enriched = (
    not_flagged
    .select(
        "*",
        F.min("balance").over(w).alias("status_min_balance"),
        F.sum("balance").over(w2).alias("cumulative_balance"),
        F.dense_rank().over(w_ordered).alias("account_type_length_dense_rank"),
    )
)

base_accounts = (
    enriched
    .filter(F.col("status").eqNullSafe(F.lit("OPEN")))
    .select(
        "*",
        F.least("account_type_length_dense_rank", "cumulative_balance").alias("least_account_type_length_dense_rank_cumulative_balance"),
        F.coalesce("status", F.lit("DIRECT")).alias("status_filled"),
    )
)

with_latest = (
    base_accounts
    .alias("s")
    .join(df_transactions.alias("c"), (F.col("c.account_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w4))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.account_type",
        "s.opened_date",
        "s.status_min_balance",
        "s.status_filled",
        F.col("c.txn_type").alias("transaction_txn_type"),
    )
)

not_flagged_accounts = (
    with_latest
    .alias("s")
    .join(df_transactions.alias("ch"), (F.col("ch.account_id") == F.col("s.id")) & (F.col("ch.txn_time") >= F.col("s.opened_date")), "left_anti")
)

result = (
    not_flagged_accounts
    .select("id", "account_type", "opened_date", "status_min_balance", "status_filled")
)

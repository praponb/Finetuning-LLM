from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy("txn_time", "id")
w2 = Window.partitionBy("status").orderBy("txn_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

joined = (
    df_transactions
    .alias("a")
    .join(df_accounts.alias("b"), F.col("a.account_id") == F.col("b.id"), "left")
    .select(
        "a.id",
        "a.account_id",
        "a.txn_type",
        "a.amount",
        "a.txn_time",
        "a.is_flagged",
        F.col("b.opened_date"),
        F.col("b.status"),
    )
)

joined_transactions = (
    joined
    .alias("a")
    .join(df_accounts.alias("d"), F.col("a.account_id") == F.col("d.id"), "left")
    .filter(F.year(F.col("d.opened_date")) == 2023)
    .select(
        "a.*",
        F.col("d.account_type"),
        F.col("d.balance"),
        F.col("d.opened_date").alias("account_opened_date"),
    )
)

joined_transactions_2 = (
    joined_transactions
    .alias("s")
    .join(df_accounts.alias("b"), F.col("s.account_id") == F.col("b.id"), "inner")
    .select("s.*", F.col("b.opened_date").alias("account_opened_date_2"))
)

with_gaps = (
    joined_transactions_2
    .withColumn("new_group_flag", F.when(F.lag("txn_time").over(w).isNull() | (F.col("txn_time") > F.lag("txn_time").over(w) + F.expr("INTERVAL 7 DAYS")), 1).otherwise(0))
)

sessionized = (
    with_gaps
    .select(
        "id",
        "account_id",
        "txn_type",
        "txn_time",
        "is_flagged",
        "status",
        "balance",
        "account_opened_date",
        "account_opened_date_2",
        F.sum("new_group_flag").over(w2).alias("burst_id"),
    )
)

result = (
    sessionized
    .rollup(F.date_trunc("year", "txn_time").alias("year_start"))
    .agg(
        F.sum(F.when(F.col("txn_type") == "DEBIT", F.col("balance"))).alias("debit_total_balance"),
        F.percentile_approx("burst_id", [0.25, 0.5, 0.75]).alias("burst_id_quartiles"),
        F.min("txn_time").alias("first_seen"),
    )
    .orderBy(F.col("year_start").desc_nulls_last())
)

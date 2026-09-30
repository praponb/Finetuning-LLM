from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("account_id").orderBy("txn_time", "id")
w2 = Window.partitionBy("account_id").orderBy("txn_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

with_dims = (
    df_transactions
    .alias("e")
    .join(df_accounts.alias("a"), F.col("e.account_id") == F.col("a.id"), "inner")
    .filter(F.col("a.account_type") == "SAVINGS")
    .select("e.*", F.col("a.status"), F.col("a.opened_date"))
)

with_dims_transactions = (
    with_dims
    .alias("src")
    .join(df_accounts.alias("a"), F.col("src.account_id") == F.col("a.id"), "inner")
    .select(
        "src.*",
        F.col("a.account_type"),
        F.col("a.opened_date").alias("account_opened_date"),
        F.col("a.balance"),
    )
)

gaps = (
    with_dims_transactions
    .withColumn("is_new_burst", F.when(F.col("txn_time") - F.lag("txn_time").over(w) > F.expr("INTERVAL 30 DAYS"), 1).otherwise(0))
)

sessions_transactions = gaps.withColumn("session_id", F.sum("is_new_burst").over(w2))

with_dims_transactions_2 = (
    sessions_transactions
    .alias("s")
    .join(df_accounts.alias("a"), F.col("s.account_id") == F.col("a.id"), "inner")
    .select(
        "s.id",
        "s.account_id",
        "s.txn_type",
        "s.opened_date",
        "s.account_type",
        "s.account_opened_date",
        "s.balance",
        "s.session_id",
        F.col("a.status").alias("account_status"),
        F.col("a.balance").alias("account_balance"),
    )
)

result = (
    with_dims_transactions_2
    .groupBy(
        F.when(F.col("balance") >= 87000, "LARGE").otherwise("SMALL").alias("balance_tier"),
    )
    .agg(
        F.size(F.collect_set("account_status")).alias("num_account_status"),
        F.count(F.when(F.col("txn_type") == "CREDIT", 1)).alias("credit_count"),
        F.sum(F.when(F.col("txn_type") == "FEE", F.col("balance")).otherwise(0)).alias("fee_balance"),
    )
    .orderBy(F.col("balance_tier").asc_nulls_last())
)

from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("account_id").orderBy("txn_time", "id")
w2 = Window.partitionBy("account_id").orderBy("txn_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("merchant", "account_id").orderBy("txn_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy(F.month("txn_time"), "merchant").orderBy("balance", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
amount_by_account_id = df_transactions.groupBy("account_id").agg(F.avg("amount").alias("avg_transaction_amount"))

with_prev = df_transactions.withColumn("prev_txn_time", F.lag("txn_time").over(w))

with_gaps = (
    with_prev
    .withColumn("new_group_flag", F.when(F.col("prev_txn_time").isNull() | (F.col("txn_time") > F.col("prev_txn_time") + F.expr("INTERVAL 14 DAYS")), 1).otherwise(0))
)

sessionized = with_gaps.withColumn("island_id", F.sum("new_group_flag").over(w2))

with_dims = (
    sessionized
    .alias("s")
    .join(df_accounts.alias("d"), F.col("s.account_id") == F.col("d.id"), "inner")
    .select("s.*", F.col("d.balance"), F.col("d.opened_date"))
)

cleaned = (
    with_dims
    .filter(F.col("merchant").isNotNull() & (F.col("balance") >= 65000))
    .withColumn("new_group_flag_scaled", F.round((F.col("new_group_flag") * 0.9), 2))
    .withColumn("island_id_minus_balance", (F.col("island_id") - F.col("balance")))
    .withColumn("merchant_clean", F.coalesce("merchant", F.lit("NONE")))
)

scored = (
    cleaned
    .filter(F.col("txn_time") >= "2025-04-01")
    .select(
        "*",
        F.min("balance").over(w3).alias("running_min_balance"),
        F.avg("balance").over(w3).alias("running_avg_balance"),
        F.max("balance").over(w3).alias("running_max_balance"),
        F.first("balance", ignorenulls=True).over(w4).alias("first_known_balance"),
    )
)

result = (
    scored
    .join(amount_by_account_id, on="account_id", how="left")
    .select(
        "id",
        "account_id",
        "txn_time",
        "merchant",
        "is_flagged",
        "prev_txn_time",
        "balance",
        "opened_date",
        "island_id_minus_balance",
        F.col("avg_transaction_amount"),
    )
)

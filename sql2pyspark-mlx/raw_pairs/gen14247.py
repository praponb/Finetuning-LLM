from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("account_type")
w_hist = Window.partitionBy(F.year("txn_time"), "account_id").orderBy("balance", "id")
w3 = Window.partitionBy("account_id", "account_type").orderBy("is_flagged_flag", "id")
w4 = Window.orderBy("account_type_count_balance", "id")
w5 = Window.partitionBy("account_type").orderBy("txn_time", "id")
w6 = Window.partitionBy("account_type").orderBy("txn_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w7 = Window.partitionBy("account_id", "account_type").orderBy(F.col("prev_balance").asc_nulls_last(), "id")

filtered = (
    df_transactions
    .filter(F.col("account_id").isin([7, 8, 18, 28]))
    .select(
        "id",
        "account_id",
        "amount",
        "txn_time",
        "merchant",
        "is_flagged",
        F.col("is_flagged").cast("int").alias("is_flagged_flag"),
        F.coalesce("merchant", F.lit("DIRECT")).alias("resolved_merchant"),
    )
)

with_dims = (
    filtered
    .alias("s")
    .join(df_accounts.alias("b"), F.col("s.account_id") == F.col("b.id"), "left")
    .select(
        "s.id",
        "s.account_id",
        "s.amount",
        "s.txn_time",
        "s.is_flagged",
        "s.is_flagged_flag",
        "s.resolved_merchant",
        F.col("b.account_type"),
        F.col("b.balance"),
    )
)

metrics = (
    with_dims
    .withColumn("account_type_count_balance", F.count("balance").over(w))
    .withColumn("next_amount", F.lead("amount", 1).over(w_hist))
)

with_windows = (
    metrics
    .withColumn("prev_next_amount", F.lag("next_amount").over(w3))
    .withColumn("prev_account_type_count_balance", F.lag("account_type_count_balance").over(w3))
    .withColumn("next_account_type_count_balance", F.lead("account_type_count_balance").over(w3))
    .withColumn("row_seq", F.row_number().over(w3))
    .withColumn("prev_balance", F.lag("balance", 1).over(w4))
)

flagged = (
    with_windows
    .withColumn("session_start_flag", F.when(F.col("txn_time") - F.lag("txn_time").over(w5) > F.expr("INTERVAL 30 DAYS"), 1).otherwise(0))
)

sessionized = flagged.withColumn("island_id", F.sum("session_start_flag").over(w6))

flagged_2 = (
    sessionized
    .withColumn("is_new_burst", F.when(F.lag("txn_time").over(w5).isNull() | (F.col("txn_time") > F.lag("txn_time").over(w5) + F.expr("INTERVAL 30 DAYS")), 1).otherwise(0))
)

sessionized_transactions = flagged_2.withColumn("session_no", F.sum("is_new_burst").over(w6))

deduped = (
    sessionized_transactions
    .withColumn("rank_in_group", F.row_number().over(w7))
    .filter(F.col("rank_in_group") == 1)
)

result = (
    deduped
    .alias("s")
    .join(df_accounts.alias("a"), F.col("s.account_id") == F.col("a.id"), "inner")
    .rollup(
        F.col("a.account_type").alias("account_account_type"),
        F.col("s.account_type").alias("transaction_account_type"),
    )
    .agg(
        F.count("*").alias("rows_total"),
        F.min(F.col("s.account_type_count_balance")).alias("min_account_type_count_balance"),
    )
    .orderBy(F.col("rows_total").desc_nulls_last())
)

from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("txn_type").orderBy("txn_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_recent = Window.partitionBy("txn_type").orderBy("amount", "id")
w3 = Window.partitionBy(F.to_date("txn_time"), "txn_type").orderBy("txn_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_part = Window.partitionBy("txn_type").orderBy(F.col("txn_time").cast("bigint")).rangeBetween(-604800, Window.currentRow)
w5 = Window.partitionBy("txn_type").orderBy("cumulative_amount", "id")
w6 = Window.partitionBy("txn_type").orderBy("cumulative_amount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w7 = Window.partitionBy("status").orderBy(F.col("first_known_amount").desc_nulls_first())
w8 = Window.partitionBy("account_id", "status").orderBy("cumulative_amount", "id")
w9 = Window.partitionBy("account_id").orderBy(F.col("prev_prev_amount").desc(), "id")
w10 = Window.partitionBy("account_id", "status").orderBy("first_known_amount", "id")
w11 = Window.partitionBy("status").orderBy("amount_max_7d", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

valid = (
    df_transactions
    .filter(F.col("merchant").isNotNull())
    .select(
        "id",
        "account_id",
        "txn_type",
        "amount",
        "txn_time",
        "is_flagged",
        F.when(F.col("txn_type").isin(["DEBIT", "FEE"]), "Core").otherwise("Other").alias("txn_type_group"),
    )
)

with_windows = (
    valid
    .select(
        "*",
        F.first("amount", ignorenulls=True).over(w).alias("first_known_amount"),
        F.lag("amount", 1).over(w_recent).alias("prev_amount"),
        F.sum("amount").over(w3).alias("cumulative_amount"),
        F.avg("amount").over(w_part).alias("amount_avg_7d"),
        F.max("amount").over(w_part).alias("amount_max_7d"),
    )
)

with_windows_transactions = (
    with_windows
    .select(
        "id",
        "account_id",
        "amount",
        "txn_type_group",
        "first_known_amount",
        "prev_amount",
        "cumulative_amount",
        "amount_max_7d",
        F.lead("amount").over(w5).alias("next_amount"),
        F.lag("amount").over(w5).alias("prev_amount_2"),
        F.lag("prev_amount").over(w5).alias("prev_prev_amount"),
        F.row_number().over(w5).alias("event_seq"),
        F.sum("prev_amount").over(w6).alias("running_prev_amount"),
    )
)

enriched = (
    with_windows_transactions
    .alias("e")
    .join(df_accounts.alias("a"), F.col("e.account_id") == F.col("a.id"), "inner")
    .select(
        "e.id",
        "e.account_id",
        "e.first_known_amount",
        "e.cumulative_amount",
        "e.amount_max_7d",
        "e.prev_prev_amount",
        "e.event_seq",
        F.col("a.status"),
        F.col("a.account_type"),
        F.col("a.opened_date"),
    )
)

latest = (
    enriched
    .withColumn("rank_in_group", F.rank().over(w7))
    .filter(F.col("rank_in_group") == 1)
)

latest_transactions = (
    latest
    .withColumn("rank_in_group_2", F.row_number().over(w8))
    .filter(F.col("rank_in_group_2") == 1)
)

deduped = (
    latest_transactions
    .withColumn("row_num", F.row_number().over(w9))
    .filter(F.col("row_num") == 1)
)

result = (
    deduped
    .withColumn("prev_prev_prev_amount", F.lag("prev_prev_amount").over(w10))
    .withColumn("prev_amount_max_7d", F.lag("amount_max_7d").over(w10))
    .withColumn("next_amount_max_7d", F.lead("amount_max_7d").over(w10))
    .withColumn("seq_no", F.row_number().over(w10))
    .withColumn("first_known_account_type", F.first("account_type", ignorenulls=True).over(w11))
)

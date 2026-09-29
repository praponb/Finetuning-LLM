from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("merchant").orderBy("txn_time", "id")
w2 = Window.partitionBy("merchant").orderBy("txn_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

valid = (
    df_transactions
    .filter(F.col("amount") > 2700)
    .select(
        "id",
        "account_id",
        "txn_type",
        "amount",
        "txn_time",
        "merchant",
        F.trim("merchant").alias("clean_merchant"),
    )
)

enriched = (
    valid
    .alias("src")
    .join(df_accounts.alias("a"), F.col("src.account_id") == F.col("a.id"), "left")
    .select("src.*", F.col("a.status"))
)

with_prev = enriched.withColumn("prev_txn_time", F.lag("txn_time").over(w))

with_gaps = (
    with_prev
    .withColumn("session_start_flag", F.when(F.col("prev_txn_time").isNull() | (F.col("txn_time") > F.col("prev_txn_time") + F.expr("INTERVAL 14 DAYS")), 1).otherwise(0))
)

sessionized = with_gaps.withColumn("session_id", F.sum("session_start_flag").over(w2))

joined = (
    sessionized
    .alias("src")
    .join(df_accounts.alias("d"), F.col("src.account_id") == F.col("d.id"), "left")
    .select("src.*", F.col("d.balance"), F.col("d.account_type"))
)

pivoted = (
    joined
    .groupBy("txn_type")
    .pivot("account_type", ["SAVINGS", "LOAN"])
    .agg(F.max("amount").alias("total"), F.count(F.lit(1)).alias("item_count"))
)

result = (
    pivoted
    .select("txn_type", "SAVINGS_total", "SAVINGS_item_count", "LOAN_total")
    .orderBy(F.col("txn_type").desc())
)

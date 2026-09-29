from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("merchant").orderBy("txn_time", "id")
w2 = Window.partitionBy("merchant").orderBy("txn_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("txn_type").orderBy("is_new_burst", "id")
w4 = Window.partitionBy("txn_type", "account_id").orderBy("amount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("merchant").orderBy(F.col("txn_time").cast("bigint")).rangeBetween(-2592000, Window.currentRow)
amount_by_account_id = df_transactions.groupBy("account_id").agg(F.min("amount").alias("min_transaction_amount"))

flagged = (
    df_transactions
    .withColumn("is_new_burst", F.when(F.col("txn_time") - F.lag("txn_time").over(w) > F.expr("INTERVAL 60 DAYS"), 1).otherwise(0))
)

sessions_transactions = flagged.withColumn("session_id", F.sum("is_new_burst").over(w2))

valid = (
    sessions_transactions
    .filter(F.col("txn_time") >= F.lit("2024-02-23").cast("date"))
    .withColumn("amount_per_session_id", (F.col("amount") / F.nullif("session_id", F.lit(0))))
)

deduped = (
    valid
    .withColumn("latest_rank", F.row_number().over(w3))
    .filter(F.col("latest_rank") == 1)
)

with_windows = (
    deduped
    .withColumn("cumulative_session_id", F.sum("session_id").over(w4))
    .withColumn("amount_per_session_id_sum_30d", F.sum("amount_per_session_id").over(w5))
)

result = (
    with_windows
    .join(amount_by_account_id, on="account_id", how="left")
    .select(
        "id",
        "account_id",
        "txn_type",
        "amount",
        "merchant",
        "is_flagged",
        "is_new_burst",
        "session_id",
        "amount_per_session_id",
        F.when(F.col("merchant").rlike("^(Grab|7-Eleven)$"), "GROUP_A").otherwise("GROUP_B").alias("merchant_zone"),
        F.col("min_transaction_amount"),
    )
)

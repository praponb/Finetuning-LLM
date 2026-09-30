from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("account_id").orderBy("txn_time", "id")
w2 = Window.partitionBy("account_id").orderBy("txn_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

base = (
    df_transactions
    .filter(F.col("merchant").isNotNull() & (F.col("amount") >= 3800))
    .select(
        "id",
        "account_id",
        "txn_type",
        "amount",
        "txn_time",
        "is_flagged",
        F.concat("merchant", F.lit(" "), "txn_type").alias("merchant_txn_type"),
        F.when(F.col("is_flagged"), "Yes").otherwise("No").alias("is_flagged_label"),
    )
)

gaps = (
    base
    .withColumn("session_start_flag", F.when(F.col("txn_time") - F.lag("txn_time").over(w) > F.expr("INTERVAL 30 DAYS"), 1).otherwise(0))
)

sessions_transactions = gaps.withColumn("session_no", F.sum("session_start_flag").over(w2))

result = (
    sessions_transactions
    .groupBy("txn_type", "session_no")
    .agg(
        F.count("*").alias("cnt"),
        F.min("session_start_flag").alias("min_session_start_flag"),
    )
)

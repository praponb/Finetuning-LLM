from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("account_id").orderBy("txn_time", "id")
w2 = Window.partitionBy("account_id").orderBy("txn_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

with_gaps = (
    df_transactions
    .withColumn("session_start_flag", F.when(F.col("txn_time") - F.lag("txn_time").over(w) > F.expr("INTERVAL 30 DAYS"), 1).otherwise(0))
)

islands = (
    with_gaps
    .select(
        "id",
        "account_id",
        "txn_type",
        "amount",
        "txn_time",
        F.sum("session_start_flag").over(w2).alias("session_no"),
    )
)

result = (
    islands
    .withColumn("week_start", F.date_trunc("WEEK", "txn_time"))
    .rollup("week_start")
    .agg(
        F.count(F.when(F.col("txn_type") == "REFUND", 1)).alias("refund_rows"),
        F.count_if(F.col("txn_type") == "DEBIT").alias("debit_events"),
        F.sum(F.when(F.col("txn_type") == "FEE", F.col("session_no")).otherwise(0)).alias("fee_session_no"),
        F.size(F.collect_set("txn_type")).alias("num_txn_types"),
        F.grouping_id().alias("gid"),
    )
    .filter(F.col("refund_rows") > 1)
    .orderBy(F.col("refund_rows").desc())
)

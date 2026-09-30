from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("account_id").orderBy("txn_time", "id")
w2 = Window.partitionBy("account_id").orderBy("txn_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("status").orderBy("txn_time", "id")
w4 = Window.partitionBy("status").orderBy("txn_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("account_id").orderBy(F.col("txn_time").cast("bigint")).rangeBetween(-2592000, Window.currentRow)
w_ordered = Window.partitionBy("txn_type").orderBy(F.col("txn_time").desc(), "id")
w7 = Window.partitionBy("status").orderBy("burst_id_2_max_30d")
w8 = Window.partitionBy("account_id").orderBy("opened_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

with_dims = (
    df_transactions
    .alias("s")
    .join(df_accounts.alias("d"), F.col("s.account_id") == F.col("d.id"), "inner")
    .select(
        "s.id",
        "s.account_id",
        "s.txn_type",
        "s.txn_time",
        "s.is_flagged",
        F.col("d.opened_date"),
        F.col("d.status"),
    )
)

with_gaps = (
    with_dims
    .withColumn("is_new_session", F.when(F.col("txn_time") - F.lag("txn_time").over(w) > F.expr("INTERVAL 60 DAYS"), 1).otherwise(0))
)

islands = with_gaps.withColumn("burst_id", F.sum("is_new_session").over(w2))

with_gaps_2 = (
    islands
    .withColumn("new_group_flag", F.when(F.col("txn_time") - F.lag("txn_time").over(w3) > F.expr("INTERVAL 14 DAYS"), 1).otherwise(0))
)

islands_transactions = with_gaps_2.withColumn("burst_id_2", F.sum("new_group_flag").over(w4))

metrics = (
    islands_transactions
    .select(
        "id",
        "account_id",
        "txn_type",
        "txn_time",
        "is_flagged",
        "opened_date",
        "status",
        "is_new_session",
        F.max("burst_id_2").over(w5).alias("burst_id_2_max_30d"),
        F.row_number().over(w_ordered).alias("event_seq"),
    )
)

result = (
    metrics
    .filter(F.col("event_seq") > 4)
    .select(
        "id",
        "txn_type",
        "is_flagged",
        "opened_date",
        "status",
        "is_new_session",
        "burst_id_2_max_30d",
        "event_seq",
        F.dense_rank().over(w7).alias("burst_id_2_max_30d_dense_rank"),
        F.sum("event_seq").over(w8).alias("running_event_seq"),
    )
)

from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("txn_type").orderBy("txn_time", "id")
w2 = Window.partitionBy("txn_type").orderBy("txn_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("merchant")
w4 = Window.orderBy("amount", "id").rowsBetween(-2, -1)
w5 = Window.partitionBy(F.year("txn_time"), "merchant").orderBy(F.col("txn_time").cast("bigint")).rangeBetween(-2592000, Window.currentRow)
w6 = Window.partitionBy(F.year("txn_time"), "account_id").orderBy("txn_time", "id")
w7 = Window.partitionBy(F.year("txn_time"), "merchant").orderBy("is_new_session", "id").rowsBetween(-5, -1)
w8 = Window.partitionBy("merchant").orderBy("next_amount_share", "id")
w_running = Window.partitionBy(F.month("txn_time"), "merchant").orderBy("is_new_session", "id")
w10 = Window.partitionBy("account_id").orderBy("amount", "id").rowsBetween(-2, Window.currentRow)
w_recent = Window.partitionBy("txn_type").orderBy("is_new_session_bucket", "id")
w12 = Window.partitionBy("merchant").orderBy("amount_share_max_30d", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_grp = Window.partitionBy(F.date_trunc("YEAR", "txn_time"), "account_id").orderBy("session_id", "id")
w14 = Window.orderBy(F.col("next_amount_share").desc(), "id")
w15 = Window.partitionBy("txn_type").orderBy(F.col("session_id_avg_5_prior").asc_nulls_first())

gaps = (
    df_transactions
    .withColumn("is_new_session", F.when(F.col("txn_time") - F.lag("txn_time").over(w) > F.expr("INTERVAL 7 DAYS"), 1).otherwise(0))
)

sessions_transactions = gaps.withColumn("session_id", F.sum("is_new_session").over(w2))

enriched = (
    sessions_transactions
    .select(
        "id",
        "account_id",
        "txn_type",
        "amount",
        "txn_time",
        "merchant",
        "is_flagged",
        "is_new_session",
        "session_id",
        F.round((F.col("amount") / F.sum("amount").over(w3)), 4).alias("amount_share"),
        F.avg("session_id").over(w4).alias("session_id_avg_2_prior"),
    )
)

metrics = (
    enriched
    .select(
        "*",
        F.max("amount_share").over(w5).alias("amount_share_max_30d"),
        F.lead("amount_share", 2).over(w6).alias("next_amount_share"),
    )
)

enriched_transactions = (
    metrics
    .select(
        "*",
        F.avg("session_id").over(w7).alias("session_id_avg_5_prior"),
        F.ntile(3).over(w8).alias("next_amount_share_bucket"),
        F.ntile(4).over(w_running).alias("is_new_session_bucket"),
        F.avg("amount").over(w10).alias("amount_avg_2"),
    )
)

with_windows = (
    enriched_transactions
    .withColumn("next_is_new_session_bucket", F.lead("is_new_session_bucket", 1, 0).over(w_recent))
    .withColumn("first_known_is_new_session_bucket", F.first("is_new_session_bucket", ignorenulls=True).over(w12))
)

with_windows_transactions = (
    with_windows
    .select(
        "*",
        F.lag("first_known_is_new_session_bucket", 1).over(w_grp).alias("prev_first_known_is_new_session_bucket"),
        F.row_number().over(w14).alias("event_seq"),
        F.cume_dist().over(w15).alias("session_id_avg_5_prior_cume_dist"),
    )
)

result = (
    with_windows_transactions
    .select(
        "id",
        "account_id",
        "txn_type",
        "amount",
        "merchant",
        "session_id",
        "next_amount_share_bucket",
        "amount_avg_2",
        "next_is_new_session_bucket",
    )
    .orderBy(F.col("next_amount_share_bucket").desc())
)

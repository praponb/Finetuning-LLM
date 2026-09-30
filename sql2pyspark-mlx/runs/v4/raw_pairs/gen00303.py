from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("txn_time", "id")
w2 = Window.partitionBy("merchant", "txn_type").orderBy("txn_time", "id").rowsBetween(-2, -1)
w3 = Window.partitionBy("account_id").orderBy("txn_time", "id")
w4 = Window.partitionBy("account_id").orderBy("txn_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("merchant").orderBy(F.col("event_seq").desc())
w6 = Window.partitionBy("txn_type").orderBy(F.col("session_id").desc(), "id")
w_running = Window.partitionBy("account_id")
w_ordered = Window.orderBy(F.col("amount_avg_2_prior").desc())
w9 = Window.orderBy("txn_time", "id").rowsBetween(-3, Window.currentRow)

scored = (
    df_transactions
    .select(
        "*",
        F.row_number().over(w).alias("event_seq"),
        F.avg("amount").over(w2).alias("amount_avg_2_prior"),
    )
)

valid = (
    scored
    .filter(F.col("merchant").isNotNull() & F.col("account_id").isin([10, 17]))
    .withColumn("merchant_length", F.length("merchant"))
    .withColumn("merchant_clean", F.coalesce("merchant", F.lit("UNKNOWN")))
)

with_gaps = (
    valid
    .withColumn("is_new_burst", F.when(F.lag("txn_time").over(w3).isNull() | (F.col("txn_time") > F.lag("txn_time").over(w3) + F.expr("INTERVAL 60 DAYS")), 1).otherwise(0))
)

islands = with_gaps.withColumn("session_id", F.sum("is_new_burst").over(w4))

ranked = (
    islands
    .withColumn("rank_in_group", F.rank().over(w5))
    .filter(F.col("rank_in_group") == 1)
)

latest = (
    ranked
    .withColumn("row_num", F.row_number().over(w6))
    .filter(F.col("row_num") == 1)
)

metrics = (
    latest
    .select(
        "*",
        F.sum("is_new_burst").over(w_running).alias("account_id_sum_is_new_burst"),
        F.cume_dist().over(w_ordered).alias("amount_avg_2_prior_cume_dist"),
        F.avg("event_seq").over(w9).alias("event_seq_avg_3"),
    )
)

valid_transactions = (
    metrics
    .filter(F.col("is_new_burst").between(0, 1))
    .select(
        "*",
        (F.col("amount_avg_2_prior") + F.col("is_new_burst")).alias("amount_avg_2_prior_plus_is_new_burst"),
        F.coalesce("merchant", F.lit("NONE")).alias("merchant_clean_2"),
    )
)

result = (
    valid_transactions
    .select(
        "id",
        "account_id",
        "txn_type",
        "merchant",
        "is_flagged",
        "event_seq",
        "amount_avg_2_prior",
        "merchant_length",
        "is_new_burst",
        F.when(F.col("merchant").rlike("^(7-Eleven|Lazada)$"), "PRIORITY").when(F.col("merchant").rlike("^(Grab|Amazon)$"), "CORE").otherwise("OTHER_ZONE").alias("merchant_zone"),
    )
)

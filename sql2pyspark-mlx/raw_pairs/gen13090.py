from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("merchant").orderBy("txn_time", "id")
w2 = Window.partitionBy("account_id", "txn_type")
w3 = Window.partitionBy("account_id").orderBy(F.col("txn_time").desc_nulls_last(), "id")
w4 = Window.partitionBy("txn_type").orderBy("txn_time", "id")
w5 = Window.partitionBy("txn_type").orderBy("txn_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.partitionBy("account_id").orderBy("amount_share", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w7 = Window.partitionBy(F.to_date("txn_time"), "txn_type").orderBy("amount_share", "id").rowsBetween(-6, Window.currentRow)
w8 = Window.partitionBy("account_id", "status")
w9 = Window.partitionBy("merchant")
w10 = Window.partitionBy("account_id").orderBy("prev_amount", "id").rowsBetween(-3, Window.currentRow)

scored = (
    df_transactions
    .filter(F.col("merchant").isNotNull())
    .withColumn("prev_amount", F.lag("amount", 1).over(w))
    .withColumn("amount_share", F.round((F.col("amount") / F.sum("amount").over(w2)), 4))
)

ranked = (
    scored
    .withColumn("latest_rank", F.row_number().over(w3))
    .filter(F.col("latest_rank") == 1)
)

gaps = (
    ranked
    .withColumn("is_new_burst", F.when(F.col("txn_time") - F.lag("txn_time").over(w4) > F.expr("INTERVAL 14 DAYS"), 1).otherwise(0))
)

sessions_transactions = gaps.withColumn("island_id", F.sum("is_new_burst").over(w5))

metrics = (
    sessions_transactions
    .select(
        "id",
        "account_id",
        "txn_time",
        "merchant",
        "is_flagged",
        "prev_amount",
        "amount_share",
        "is_new_burst",
        F.sum("prev_amount").over(w6).alias("cumulative_prev_amount"),
        F.avg("is_new_burst").over(w7).alias("is_new_burst_avg_6"),
    )
)

enriched = (
    metrics
    .alias("s")
    .join(df_accounts.alias("d"), F.col("s.account_id") == F.col("d.id"), "left")
    .select("s.*", F.col("d.opened_date"), F.col("d.status"), F.col("d.balance"))
)

result = (
    enriched
    .select(
        "id",
        "account_id",
        "txn_time",
        "merchant",
        "is_flagged",
        "cumulative_prev_amount",
        "opened_date",
        "status",
        F.stddev_samp("is_new_burst").over(w8).alias("account_id_stddev_samp_is_new_burst"),
        F.round((F.col("cumulative_prev_amount") / F.sum("cumulative_prev_amount").over(w9)), 4).alias("cumulative_prev_amount_share"),
        F.avg("amount_share").over(w10).alias("amount_share_avg_3"),
    )
)

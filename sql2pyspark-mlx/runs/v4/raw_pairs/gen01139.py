from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("account_id").orderBy(F.col("is_flagged_flag").asc_nulls_last(), "id")
w2 = Window.partitionBy("txn_type", "account_id")
w_grp = Window.partitionBy("txn_type", "merchant").orderBy(F.col("txn_time").cast("bigint")).rangeBetween(-2592000, Window.currentRow)
w4 = Window.orderBy("txn_time", "id")
w_grp_2 = Window.partitionBy("merchant").orderBy("txn_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.partitionBy("merchant").orderBy(F.col("is_flagged_flag_2_sum_30d").desc(), "id")
w7 = Window.orderBy(F.col("amount").desc(), "id")
w8 = Window.partitionBy("account_id", "txn_type").orderBy("dedup_rank", "id").rowsBetween(-2, Window.currentRow)
w9 = Window.partitionBy("merchant").orderBy("txn_time", "id")

filtered = (
    df_transactions
    .filter(F.col("amount") <= 3400)
    .select(
        "*",
        F.col("is_flagged").cast("int").alias("is_flagged_flag"),
        F.col("is_flagged").cast("int").alias("is_flagged_flag_2"),
    )
)

deduped = (
    filtered
    .withColumn("dedup_rank", F.row_number().over(w))
    .filter(F.col("dedup_rank") == 1)
)

scored = (
    deduped
    .withColumn("amount_share", F.round((F.col("amount") / F.sum("amount").over(w2)), 4))
    .withColumn("is_flagged_flag_2_sum_30d", F.sum("is_flagged_flag_2").over(w_grp))
    .withColumn("next_is_flagged_flag_2", F.lead("is_flagged_flag_2", 1).over(w4))
    .withColumn("running_is_flagged_flag", F.sum("is_flagged_flag").over(w_grp_2))
)

with_windows = (
    scored
    .select(
        "*",
        F.row_number().over(w6).alias("seq_no"),
        F.row_number().over(w7).alias("row_seq"),
        F.avg("next_is_flagged_flag_2").over(w8).alias("next_is_flagged_flag_2_avg_2"),
    )
)

joined = (
    with_windows
    .alias("s")
    .join(df_accounts.alias("p"), F.col("s.account_id") == F.col("p.id"), "left")
    .select("s.*", F.col("p.opened_date"), F.col("p.status"), F.col("p.balance"))
)

with_gaps = (
    joined
    .withColumn("new_group_flag", F.when(F.col("txn_time") - F.lag("txn_time").over(w9) > F.expr("INTERVAL 60 DAYS"), 1).otherwise(0))
)

islands = with_gaps.withColumn("island_id", F.sum("new_group_flag").over(w_grp_2))

with_dims = (
    islands
    .alias("e")
    .join(df_accounts.alias("b"), F.col("e.account_id") == F.col("b.id"), "left")
    .select(
        "e.*",
        F.col("b.status").alias("account_status"),
        F.col("b.balance").alias("account_balance"),
        F.col("b.opened_date").alias("account_opened_date"),
    )
)

result = (
    with_dims
    .groupBy(F.date_trunc("YEAR", "opened_date").alias("year_start"))
    .agg(
        F.stddev_samp("amount").alias("amount_stddev"),
        ((F.unix_timestamp(F.max("txn_time")) - F.unix_timestamp(F.min("txn_time"))) / 60).alias("duration_min"),
        F.percentile_approx("balance", [0.25, 0.5, 0.75]).alias("balance_quartiles"),
        F.max("txn_time").alias("last_seen"),
        F.min("txn_time").alias("first_txn_time"),
        F.count("*").alias("row_count"),
    )
)

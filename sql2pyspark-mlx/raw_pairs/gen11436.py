from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("amount_whole", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("account_id")
w3 = Window.orderBy("txn_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("account_id", "merchant").orderBy(F.col("txn_time").desc(), "id")
w_hist = Window.partitionBy("txn_type").orderBy("amount_whole", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.partitionBy("merchant")
w7 = Window.partitionBy("account_id").orderBy("running_sum_account_id_stddev_samp_amount_whole", "id")
w8 = Window.partitionBy("merchant").orderBy(F.col("cumulative_account_id_stddev_samp_amount_whole").desc(), "id")
w_grp = Window.partitionBy("account_id").orderBy(F.col("amount").desc_nulls_first())
w_part = Window.orderBy("merchant_stddev_samp_running_sum_account_id_stddev_samp_amount_whole", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

filtered = (
    df_transactions
    .filter(F.col("merchant").isNotNull() & ~F.col("is_flagged"))
    .select(
        "id",
        "account_id",
        "txn_type",
        "amount",
        "txn_time",
        "merchant",
        "is_flagged",
        F.lower("txn_type").alias("txn_type_lower"),
        F.floor("amount").alias("amount_whole"),
        F.coalesce("merchant", F.lit("OTHER")).alias("merchant_filled"),
    )
)

enriched = (
    filtered
    .select(
        "*",
        F.sum("amount").over(w).alias("running_amount"),
        F.stddev_samp("amount_whole").over(w2).alias("account_id_stddev_samp_amount_whole"),
        F.last("amount_whole", ignorenulls=True).over(w3).alias("last_known_amount_whole"),
        F.row_number().over(w4).alias("row_seq"),
    )
)

scored = (
    enriched
    .select(
        "id",
        "account_id",
        "amount",
        "merchant",
        "txn_type_lower",
        "merchant_filled",
        "last_known_amount_whole",
        "row_seq",
        F.avg("account_id_stddev_samp_amount_whole").over(w_hist).alias("running_avg_account_id_stddev_samp_amount_whole"),
        F.sum("account_id_stddev_samp_amount_whole").over(w_hist).alias("running_sum_account_id_stddev_samp_amount_whole"),
        F.max("account_id_stddev_samp_amount_whole").over(w_hist).alias("running_max_account_id_stddev_samp_amount_whole"),
        F.sum("account_id_stddev_samp_amount_whole").over(w).alias("cumulative_account_id_stddev_samp_amount_whole"),
    )
)

with_windows = (
    scored
    .select(
        "id",
        "merchant_filled",
        "running_sum_account_id_stddev_samp_amount_whole",
        "running_max_account_id_stddev_samp_amount_whole",
        F.stddev_samp("running_sum_account_id_stddev_samp_amount_whole").over(w6).alias("merchant_stddev_samp_running_sum_account_id_stddev_samp_amount_whole"),
        F.lead("running_max_account_id_stddev_samp_amount_whole", 1).over(w7).alias("next_running_max_account_id_stddev_samp_amount_whole"),
        F.ntile(4).over(w8).alias("cumulative_account_id_stddev_samp_amount_whole_bucket"),
        F.percent_rank().over(w_grp).alias("amount_percent_rank"),
        F.count("running_sum_account_id_stddev_samp_amount_whole").over(w2).alias("account_id_count_running_sum_account_id_stddev_samp_amount_whole"),
    )
)

scored_transactions = (
    with_windows
    .select(
        "id",
        "merchant_stddev_samp_running_sum_account_id_stddev_samp_amount_whole",
        "amount_percent_rank",
        "account_id_count_running_sum_account_id_stddev_samp_amount_whole",
        F.first("running_sum_account_id_stddev_samp_amount_whole", ignorenulls=True).over(w_part).alias("first_known_running_sum_account_id_stddev_samp_amount_whole"),
        F.first("running_max_account_id_stddev_samp_amount_whole", ignorenulls=True).over(w_part).alias("first_known_running_max_account_id_stddev_samp_amount_whole"),
    )
)

base = (
    scored_transactions
    .filter(F.col("first_known_running_sum_account_id_stddev_samp_amount_whole") <= 120)
    .withColumn("greatest_amount_percent_rank_first_known_running_sum_account_id_stddev_samp_amount_whole", F.greatest("amount_percent_rank", "first_known_running_sum_account_id_stddev_samp_amount_whole"))
    .withColumn("first_known_running_max_account_id_stddev_samp_amount_whole_plus_amount_percent_rank", (F.col("first_known_running_max_account_id_stddev_samp_amount_whole") + F.col("amount_percent_rank")))
)

cleaned = (
    base
    .filter(F.col("greatest_amount_percent_rank_first_known_running_sum_account_id_stddev_samp_amount_whole").between(27, 30))
    .select(
        "id",
        "account_id_count_running_sum_account_id_stddev_samp_amount_whole",
        "first_known_running_sum_account_id_stddev_samp_amount_whole",
        "first_known_running_max_account_id_stddev_samp_amount_whole",
        "greatest_amount_percent_rank_first_known_running_sum_account_id_stddev_samp_amount_whole",
        "first_known_running_max_account_id_stddev_samp_amount_whole_plus_amount_percent_rank",
        F.col("greatest_amount_percent_rank_first_known_running_sum_account_id_stddev_samp_amount_whole").cast("int").alias("greatest_amount_percent_rank_first_known_running_sum_account_id_stddev_samp_amount_whole_int"),
    )
)

totals = (
    cleaned
    .groupBy(
        F.when(F.col("greatest_amount_percent_rank_first_known_running_sum_account_id_stddev_samp_amount_whole") >= 79, "HIGH").otherwise("LOW").alias("greatest_amount_percent_rank_first_known_running_sum_account_id_stddev_samp_amount_whole_tier"),
    )
    .agg(
        F.count("*").alias("event_count"),
        F.countDistinct("id").alias("distinct_ids"),
        F.round(F.avg("first_known_running_max_account_id_stddev_samp_amount_whole_plus_amount_percent_rank"), 2).alias("avg_first_known_running_max_account_id_stddev_samp_amount_whole_plus_amount_percent_rank"),
        F.percentile_approx("account_id_count_running_sum_account_id_stddev_samp_amount_whole", [0.25, 0.5, 0.75]).alias("account_id_count_running_sum_account_id_stddev_samp_amount_whole_quartiles"),
        F.sum("greatest_amount_percent_rank_first_known_running_sum_account_id_stddev_samp_amount_whole_int").alias("total_greatest_amount_percent_rank_first_known_running_sum_account_id_stddev_samp_amount_whole_int"),
    )
    .filter(F.col("event_count") > 1)
)

result = (
    totals
    .select(
        "greatest_amount_percent_rank_first_known_running_sum_account_id_stddev_samp_amount_whole_tier",
        "distinct_ids",
        "avg_first_known_running_max_account_id_stddev_samp_amount_whole_plus_amount_percent_rank",
        "account_id_count_running_sum_account_id_stddev_samp_amount_whole_quartiles",
        "total_greatest_amount_percent_rank_first_known_running_sum_account_id_stddev_samp_amount_whole_int",
    )
    .orderBy(
        F.col("distinct_ids").desc(),
        "greatest_amount_percent_rank_first_known_running_sum_account_id_stddev_samp_amount_whole_tier",
    )
)

from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("account_id", "merchant").orderBy(F.col("txn_time").asc_nulls_last(), "id")
w2 = Window.orderBy("txn_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("merchant").orderBy(F.col("amount").asc_nulls_last())
w4 = Window.partitionBy("account_id")
w_ordered = Window.orderBy("rank_in_group_per_amount_cume_dist", "id")
w_hist = Window.orderBy("amount_cume_dist_whole", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w7 = Window.orderBy("amount_cume_dist_whole")

deduped = (
    df_transactions
    .withColumn("rank_in_group", F.row_number().over(w))
    .filter(F.col("rank_in_group") == 1)
)

enriched = (
    deduped
    .select(
        "id",
        "amount",
        "txn_time",
        "rank_in_group",
        F.first("merchant", ignorenulls=True).over(w2).alias("first_known_merchant"),
        F.cume_dist().over(w3).alias("amount_cume_dist"),
        F.round((F.col("amount") / F.sum("amount").over(w4)), 4).alias("amount_share"),
    )
)

valid = (
    enriched
    .filter((F.col("txn_time") >= F.to_date(F.lit("2023-09-17"))) & (F.to_date("txn_time") >= "2025-09-01"))
    .select(
        "*",
        (F.col("rank_in_group") / F.nullif("amount_cume_dist", F.lit(0))).alias("rank_in_group_per_amount_cume_dist"),
    )
)

filtered = (
    valid
    .filter(F.col("txn_time") >= F.lit("2023-07-17").cast("date"))
    .select(
        "id",
        "amount",
        "rank_in_group",
        "first_known_merchant",
        "amount_share",
        "rank_in_group_per_amount_cume_dist",
        F.ceil("amount_cume_dist").alias("amount_cume_dist_whole"),
    )
)

scored = (
    filtered
    .filter(F.col("amount_share") < 0.32)
    .select(
        "id",
        "rank_in_group",
        "rank_in_group_per_amount_cume_dist",
        "amount_cume_dist_whole",
        F.lag("amount_share", 2).over(w_ordered).alias("prev_amount_share"),
        F.sum("amount_cume_dist_whole").over(w_hist).alias("running_amount_cume_dist_whole"),
        F.dense_rank().over(w7).alias("amount_cume_dist_whole_dense_rank"),
    )
)

valid_transactions = (
    scored
    .filter(F.col("rank_in_group_per_amount_cume_dist") <= 42)
    .select(
        "*",
        (F.col("rank_in_group_per_amount_cume_dist") / F.nullif("prev_amount_share", F.lit(0))).alias("rank_in_group_per_amount_cume_dist_per_prev_amount_share"),
        (F.col("amount_cume_dist_whole") + F.col("running_amount_cume_dist_whole")).alias("amount_cume_dist_whole_plus_running_amount_cume_dist_whole"),
    )
)

result = (
    valid_transactions
    .groupBy(
        F.when(F.col("rank_in_group_per_amount_cume_dist_per_prev_amount_share") >= 76, "HIGH").otherwise("LOW").alias("rank_in_group_per_amount_cume_dist_per_prev_amount_share_tier"),
    )
    .agg(
        F.percentile_approx("amount_cume_dist_whole_plus_running_amount_cume_dist_whole", [0.25, 0.5, 0.75]).alias("amount_cume_dist_whole_plus_running_amount_cume_dist_whole_quartiles"),
        F.countDistinct("id").alias("distinct_ids"),
        F.countDistinct("id").alias("distinct_ids_2"),
        F.percentile_approx("amount_cume_dist_whole_dense_rank", 0.95).alias("amount_cume_dist_whole_dense_rank_p95"),
        F.count("*").alias("event_count"),
        F.countDistinct("id").alias("distinct_ids_3"),
    )
    .filter(F.col("distinct_ids_2") > 1)
    .orderBy("distinct_ids_3")
)

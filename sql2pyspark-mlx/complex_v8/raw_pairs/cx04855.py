from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_reported_at = Window.orderBy("reported_at", "id")
w_all_by_claimed_amount_bucket = Window.orderBy(F.col("claimed_amount_bucket").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_reported_at_full = Window.orderBy("reported_at", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_policy_id = Window.partitionBy("policy_id")
w_all_by_claimed_amount_bucket_2 = Window.orderBy(F.col("claimed_amount_bucket").desc(), "id").rowsBetween(-3, Window.currentRow)
w_all_by_claimed_amount_bucket_3 = Window.orderBy(F.col("claimed_amount_bucket").desc(), "id")
w_adjuster_id_by_approved_amou = Window.partitionBy("adjuster_id").orderBy("approved_amount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_reported_at_ties = Window.orderBy("reported_at")
w_reported_at_dense_rank_claim = Window.partitionBy("reported_at_dense_rank", "claim_type").orderBy("approved_amount", "id")
w_reported_at_dense_rank_claim_2 = Window.partitionBy("reported_at_dense_rank", "claim_type").orderBy("approved_amount", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_second_claimed_amount = Window.orderBy("second_claimed_amount_bucket", "id").rowsBetween(-6, -1)
w_all_by_second_claimed_amount_2 = Window.orderBy("second_claimed_amount_bucket", "id")
w_all_by_last_known_reported_a = Window.orderBy("last_known_reported_at")

recent_claims = (
    df_claims
    .filter(F.col("status") == "IN_REVIEW")
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_type",
        "approved_amount",
        "reported_at",
        F.concat(F.col("claim_type"), F.lit("#"), F.col("policy_id").cast("string")).alias("claim_type_ref"),
        (F.floor(F.col("claimed_amount") / 10000) * 10000).alias("claimed_amount_bucket"),
    )
)

with_windows = (
    recent_claims
    .select(
        "id",
        "adjuster_id",
        "claim_type",
        "approved_amount",
        "claim_type_ref",
        "claimed_amount_bucket",
        F.row_number().over(w_all_by_reported_at).alias("seq_no"),
        F.last("reported_at", ignorenulls=True).over(w_all_by_claimed_amount_bucket).alias("last_known_reported_at"),
        F.nth_value("claimed_amount_bucket", 2).over(w_all_by_reported_at_full).alias("second_claimed_amount_bucket"),
        F.size(F.collect_set("adjuster_id").over(w_policy_id)).alias("distinct_adjuster_id_in_policy_id"),
        F.round(F.avg("claimed_amount_bucket").over(w_all_by_claimed_amount_bucket_2), 2).alias("claimed_amount_bucket_avg_4"),
        F.max("approved_amount").over(w_policy_id).alias("policy_id_max_approved_amount"),
        (F.col("claimed_amount_bucket") - F.lag("claimed_amount_bucket").over(w_all_by_claimed_amount_bucket_3)).alias("claimed_amount_bucket_change"),
        F.round(F.col("claimed_amount_bucket") - F.avg("claimed_amount_bucket").over(w_policy_id), 2).alias("claimed_amount_bucket_vs_policy_id_avg"),
        F.max("claimed_amount_bucket").over(w_adjuster_id_by_approved_amou).alias("max_claimed_amount_bucket_so_far"),
        F.round(F.col("approved_amount") - F.avg("approved_amount").over(w_policy_id), 2).alias("approved_amount_vs_policy_id_avg"),
        F.dense_rank().over(w_all_by_reported_at_ties).alias("reported_at_dense_rank"),
        F.lag("reported_at", 2).over(w_all_by_claimed_amount_bucket_3).alias("prev2_reported_at"),
    )
)

leaders = (
    with_windows
    .filter((F.col("seq_no") == 1) & (F.col("approved_amount") < 15857.9))
    .drop("seq_no")
)

metrics = (
    leaders
    .select(
        "*",
        F.round((F.unix_timestamp("last_known_reported_at") - F.unix_timestamp(F.lag("last_known_reported_at").over(w_reported_at_dense_rank_claim))) / 60, 1).alias("minutes_since_prev_last_known_reported_at"),
        F.nth_value("second_claimed_amount_bucket", 2).over(w_reported_at_dense_rank_claim_2).alias("second_second_claimed_amount_bucket"),
        F.round(F.avg("reported_at_dense_rank").over(w_all_by_second_claimed_amount), 2).alias("reported_at_dense_rank_avg_6_prior"),
        F.lag("approved_amount").over(w_all_by_second_claimed_amount_2).alias("prev_approved_amount"),
        F.round(F.percent_rank().over(w_all_by_last_known_reported_a), 3).alias("last_known_reported_at_percent_rank"),
    )
)

stats = (
    metrics
    .groupBy(
        F.year("last_known_reported_at").alias("last_known_reported_at_year"),
        "claim_type",
    )
    .agg(
        F.count("*").alias("cnt"),
        F.min("claimed_amount_bucket_vs_policy_id_avg").alias("min_claimed_amount_bucket_vs_policy_id_avg"),
        F.round(F.sum("minutes_since_prev_last_known_reported_at") / F.nullif(F.sum("max_claimed_amount_bucket_so_far"), F.lit(0)), 3).alias("minutes_since_prev_last_known_reported_at_to_max"),
        F.round(F.avg("prev_approved_amount"), 2).alias("avg_prev_approved_amount"),
        F.sum(F.when(F.col("claim_type").isin("fire", "collision"), F.col("claimed_amount_bucket_vs_policy_id_avg"))).alias("claimed_amount_bucket_vs_policy_id_avg_claim_typ"),
    )
)

result = (
    stats
    .filter(F.col("claim_type").isin("fire", "collision", "medical"))
    .select(
        "last_known_reported_at_year",
        "claim_type",
        "cnt",
        "min_claimed_amount_bucket_vs_policy_id_avg",
        "minutes_since_prev_last_known_reported_at_to_max",
        "avg_prev_approved_amount",
        "claimed_amount_bucket_vs_policy_id_avg_claim_typ",
    )
    .orderBy(
        "minutes_since_prev_last_known_reported_at_to_max",
        "last_known_reported_at_year",
        "claim_type",
    )
    .limit(5)
)

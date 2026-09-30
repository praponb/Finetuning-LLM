from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_claim_type_adjuster_level_by = Window.partitionBy("claim_type", "adjuster_level").orderBy("reported_at", "id")
w_reported_at_to_date_policy_i = Window.partitionBy(F.to_date("reported_at"), "policy_id").orderBy(F.col("approved_amount").desc(), "id")
w_adjuster_id_by_approved_amou = Window.partitionBy("adjuster_id").orderBy("approved_amount", "id")
w_adjuster_id = Window.partitionBy("adjuster_id")
w_reported_at_to_date_policy_i_2 = Window.partitionBy(F.to_date("reported_at"), "policy_id").orderBy(F.col("approved_amount").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_approved_amount_cume = Window.orderBy("approved_amount_cume_dist_to_greatest_approved_a", "adjuster_id", "second_reported_at_ym", "approved_amount_dense_rank").rowsBetween(-1, -1)
w_n_rows_by_stddev_seq_no_ties = Window.partitionBy("n_rows").orderBy(F.col("stddev_seq_no").asc_nulls_last())
w_n_rows_by_stddev_seq_no_runn = Window.partitionBy("n_rows").orderBy(F.col("stddev_seq_no").asc_nulls_last(), "adjuster_id", "second_reported_at_ym", "approved_amount_dense_rank").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_n_rows_by_stddev_seq_no = Window.partitionBy("n_rows").orderBy(F.col("stddev_seq_no").asc_nulls_last(), "adjuster_id", "second_reported_at_ym", "approved_amount_dense_rank")

prepared_claims = (
    df_claims
    .filter((F.col("claimed_amount") < 40639.7) & (F.col("reported_at") < F.lit("2025-02-18 13:00:00").cast("timestamp")))
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "claim_type",
        "claimed_amount",
        "approved_amount",
        "reported_at",
        F.greatest(F.col("approved_amount"), F.col("claimed_amount")).alias("greatest_approved_amount_claimed_amount"),
    )
)

joined = (
    prepared_claims.alias("s")
    .join(df_adjusters.alias("a"), (F.col("a.id") == F.col("s.adjuster_id")) & (F.col("a.team").isNotNull()), "inner")
    .filter(F.length(F.trim(F.col("a.adjuster_name"))) >= 7)
    .select(
        "s.id",
        "s.policy_id",
        "s.adjuster_id",
        "s.claim_type",
        "s.claimed_amount",
        "s.approved_amount",
        "s.reported_at",
        "s.greatest_approved_amount_claimed_amount",
        F.col("a.level").alias("adjuster_level"),
        "a.adjuster_name",
    )
)

with_windows = (
    joined
    .select(
        "id",
        "adjuster_id",
        "claimed_amount",
        "greatest_approved_amount_claimed_amount",
        F.round((F.unix_timestamp("reported_at") - F.unix_timestamp(F.lag("reported_at").over(w_claim_type_adjuster_level_by))) / 60, 1).alias("minutes_since_prev_reported_at"),
        F.row_number().over(w_reported_at_to_date_policy_i).alias("seq_no"),
        F.row_number().over(w_claim_type_adjuster_level_by).alias("reported_at_seq"),
        F.dense_rank().over(w_reported_at_to_date_policy_i).alias("approved_amount_dense_rank"),
        F.round(F.cume_dist().over(w_adjuster_id_by_approved_amou), 3).alias("approved_amount_cume_dist"),
        (F.col("claimed_amount") - F.lag("claimed_amount").over(w_claim_type_adjuster_level_by)).alias("claimed_amount_change"),
        F.max("claimed_amount").over(w_adjuster_id).alias("adjuster_id_max_claimed_amount"),
        F.size(F.collect_set("policy_id").over(w_adjuster_id)).alias("distinct_policy_id_in_adjuster_id"),
        F.nth_value("reported_at", 2).over(w_reported_at_to_date_policy_i_2).alias("second_reported_at"),
    )
)

grouped = (
    with_windows
    .groupBy(
        "adjuster_id",
        F.date_format("second_reported_at", "yyyy-MM").alias("second_reported_at_ym"),
        "approved_amount_dense_rank",
    )
    .agg(
        F.round(F.stddev("seq_no"), 2).alias("stddev_seq_no"),
        F.round(F.sum("approved_amount_cume_dist") / F.nullif(F.sum("greatest_approved_amount_claimed_amount"), F.lit(0)), 3).alias("approved_amount_cume_dist_to_greatest_approved_a"),
        F.max("second_reported_at").alias("last_second_reported_at"),
        F.count("*").alias("n_rows"),
        F.countDistinct("adjuster_id").alias("distinct_adjuster_ids"),
    )
)

with_trends = (
    grouped
    .select(
        "*",
        F.round(F.avg("distinct_adjuster_ids").over(w_all_by_approved_amount_cume), 2).alias("distinct_adjuster_ids_avg_1_prior"),
        F.round(F.cume_dist().over(w_n_rows_by_stddev_seq_no_ties), 3).alias("stddev_seq_no_cume_dist"),
        F.last("last_second_reported_at", ignorenulls=True).over(w_n_rows_by_stddev_seq_no_runn).alias("last_known_last_second_reported_at"),
        F.rank().over(w_n_rows_by_stddev_seq_no).alias("stddev_seq_no_rank"),
    )
)

result = (
    with_trends
    .select(
        "adjuster_id",
        "second_reported_at_ym",
        "stddev_seq_no",
        "distinct_adjuster_ids",
        "distinct_adjuster_ids_avg_1_prior",
        "stddev_seq_no_cume_dist",
        (F.floor(F.col("stddev_seq_no") / 2) * 2).alias("stddev_seq_no_bucket"),
    )
    .orderBy(
        F.col("stddev_seq_no").desc_nulls_first(),
        F.col("second_reported_at_ym").desc(),
    )
)

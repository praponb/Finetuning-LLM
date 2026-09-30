from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_adjuster_id_status = Window.partitionBy("adjuster_id", "status")
w_claim_type_by_claimed_amount = Window.partitionBy("claim_type").orderBy("claimed_amount_filled", "id").rowsBetween(-1, Window.currentRow)
w_claim_type_by_claimed_amount_2 = Window.partitionBy("claim_type").orderBy("claimed_amount_filled")
w_claim_type_adjuster_id_by_cl = Window.partitionBy("claim_type", "adjuster_id").orderBy("claimed_amount_filled", "id")
w_claim_type_by_claimed_amount_3 = Window.partitionBy("claim_type").orderBy("claimed_amount_filled", "id")
w_claimed_amount_filled_rank_p = Window.partitionBy("claimed_amount_filled_rank", F.date_trunc("month", "prev2_claim_date_plus_6m")).orderBy("claimed_amount_filled_rank")
w_prev2_claim_date_plus_6m_yea = Window.partitionBy(F.year("prev2_claim_date_plus_6m")).orderBy("claimed_amount_filled", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_claimed_amount_filled_rank = Window.partitionBy("claimed_amount_filled_rank")
w_adjuster_id_prev_reported_at = Window.partitionBy("adjuster_id", F.to_date("prev_reported_at")).orderBy("claimed_amount_filled_dense_rank", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_adjuster_id = Window.partitionBy("adjuster_id")
w_claimed_amount_filled_rank_d = Window.partitionBy("claimed_amount_filled_rank_dense_rank").orderBy(F.col("prev2_claim_date_plus_6m_span_days").asc_nulls_last())
w_claimed_amount_filled_rank_d_2 = Window.partitionBy("claimed_amount_filled_rank_dense_rank").orderBy("prev2_claim_date_plus_6m_span_days", "claimed_amount_filled_rank_dense_rank")
w_claimed_amount_filled_rank_d_3 = Window.partitionBy("claimed_amount_filled_rank_dense_rank").orderBy(F.col("prev2_claim_date_plus_6m_span_days").asc_nulls_last(), "claimed_amount_filled_rank_dense_rank")

recent_claims = (
    df_claims
    .filter((F.col("claim_type").isin("medical", "theft", "delay")) & (~F.col("status").isin("OPEN", "APPROVED")))
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_type",
        "status",
        "reported_at",
        F.add_months(F.col("claim_date"), 6).alias("claim_date_plus_6m"),
        F.coalesce(F.col("claimed_amount"), F.lit(0.0)).alias("claimed_amount_filled"),
    )
)

ranked = (
    recent_claims
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claimed_amount_filled",
        F.size(F.collect_set("claim_type").over(w_adjuster_id_status)).alias("distinct_claim_type_in_adjuster_id_status"),
        F.sum("claimed_amount_filled").over(w_adjuster_id_status).alias("adjuster_id_status_sum_claimed_amount_filled"),
        F.round(F.col("claimed_amount_filled") / F.nullif(F.sum("claimed_amount_filled").over(w_adjuster_id_status), F.lit(0)), 4).alias("claimed_amount_filled_share_of_adjuster_id_statu"),
        F.round(F.col("claimed_amount_filled") - F.avg("claimed_amount_filled").over(w_adjuster_id_status), 2).alias("claimed_amount_filled_vs_adjuster_id_status_avg"),
        F.sum("claimed_amount_filled").over(w_claim_type_by_claimed_amount).alias("claimed_amount_filled_sum_2"),
        F.rank().over(w_claim_type_by_claimed_amount_2).alias("claimed_amount_filled_rank"),
        F.lag("claim_date_plus_6m", 2).over(w_claim_type_adjuster_id_by_cl).alias("prev2_claim_date_plus_6m"),
        F.ntile(4).over(w_claim_type_adjuster_id_by_cl).alias("claimed_amount_filled_quartile"),
        F.count(F.lit(1)).over(w_adjuster_id_status).alias("rows_in_adjuster_id_status"),
        F.lag("reported_at").over(w_claim_type_by_claimed_amount_3).alias("prev_reported_at"),
        F.dense_rank().over(w_claim_type_by_claimed_amount_2).alias("claimed_amount_filled_dense_rank"),
    )
)

with_trends = (
    ranked
    .select(
        "id",
        "policy_id",
        "distinct_claim_type_in_adjuster_id_status",
        "adjuster_id_status_sum_claimed_amount_filled",
        "claimed_amount_filled_share_of_adjuster_id_statu",
        "claimed_amount_filled_vs_adjuster_id_status_avg",
        "claimed_amount_filled_sum_2",
        "prev2_claim_date_plus_6m",
        "claimed_amount_filled_quartile",
        "rows_in_adjuster_id_status",
        "claimed_amount_filled_dense_rank",
        F.dense_rank().over(w_claimed_amount_filled_rank_p).alias("claimed_amount_filled_rank_dense_rank"),
        F.last("claimed_amount_filled_sum_2").over(w_prev2_claim_date_plus_6m_yea).alias("last_claimed_amount_filled_sum_2"),
        F.size(F.collect_set("adjuster_id").over(w_claimed_amount_filled_rank)).alias("distinct_adjuster_id_in_claimed_amount_filled_ra"),
        F.sum("claimed_amount_filled_rank").over(w_adjuster_id_prev_reported_at).alias("running_claimed_amount_filled_rank"),
        F.round(F.avg("claimed_amount_filled_sum_2").over(w_adjuster_id), 2).alias("adjuster_id_avg_claimed_amount_filled_sum_2"),
    )
)

stats = (
    with_trends
    .filter(F.col("adjuster_id_avg_claimed_amount_filled_sum_2") < 97535.0)
    .groupBy("claimed_amount_filled_rank_dense_rank")
    .agg(
        F.min("rows_in_adjuster_id_status").alias("min_rows_in_adjuster_id_status"),
        F.datediff(F.max("prev2_claim_date_plus_6m"), F.min("prev2_claim_date_plus_6m")).alias("prev2_claim_date_plus_6m_span_days"),
        F.max("distinct_adjuster_id_in_claimed_amount_filled_ra").alias("max_distinct_adjuster_id_in_claimed_amount_fille"),
    )
    .filter(F.col("max_distinct_adjuster_id_in_claimed_amount_fille") > 2)
    .drop("max_distinct_adjuster_id_in_claimed_amount_fille")
)

metrics = (
    stats
    .select(
        "*",
        F.rank().over(w_claimed_amount_filled_rank_d).alias("prev2_claim_date_plus_6m_span_days_rank"),
        F.round(F.cume_dist().over(w_claimed_amount_filled_rank_d_2), 3).alias("prev2_claim_date_plus_6m_span_days_cume_dist"),
        F.round(F.cume_dist().over(w_claimed_amount_filled_rank_d_3), 3).alias("prev2_claim_date_plus_6m_span_days_cume_dist_2"),
    )
)

result = (
    metrics
    .filter(F.col("prev2_claim_date_plus_6m_span_days").between(175, 525))
    .select(
        "claimed_amount_filled_rank_dense_rank",
        "min_rows_in_adjuster_id_status",
        "prev2_claim_date_plus_6m_span_days_rank",
        "prev2_claim_date_plus_6m_span_days_cume_dist",
        "prev2_claim_date_plus_6m_span_days_cume_dist_2",
    )
    .orderBy(
        F.col("prev2_claim_date_plus_6m_span_days_rank").asc(),
        F.col("min_rows_in_adjuster_id_status").desc(),
        "claimed_amount_filled_rank_dense_rank",
    )
    .limit(5)
)

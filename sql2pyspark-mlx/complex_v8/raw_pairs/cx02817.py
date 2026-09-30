from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_approved_amount = Window.orderBy(F.col("approved_amount").desc_nulls_first(), "id")
w_all_by_approved_amount_full = Window.orderBy(F.col("approved_amount").desc_nulls_first(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_status_or_unknown = Window.partitionBy("status_or_unknown")

cleaned_claims = (
    df_claims
    .filter((F.col("status").isin("IN_REVIEW", "OPEN", "PAID")) & (F.col("approved_amount") < 16404.6))
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "claim_type",
        "approved_amount",
        "status",
        "reported_at",
        F.when(F.col("approved_amount") >= 14595.6, "A").when(F.col("approved_amount") >= 492.6, "B").otherwise("C").alias("approved_amount_tier"),
        F.coalesce(F.col("status"), F.lit("UNKNOWN")).alias("status_or_unknown"),
        F.concat(F.col("status"), F.lit("#"), F.col("policy_id").cast("string")).alias("status_ref"),
    )
)

ranked = (
    cleaned_claims
    .select(
        "*",
        F.lead("claim_date").over(w_all_by_approved_amount).alias("next_claim_date"),
        F.round(F.percent_rank().over(w_all_by_approved_amount), 3).alias("approved_amount_percent_rank"),
        F.last("claim_date").over(w_all_by_approved_amount_full).alias("last_claim_date"),
        F.round((F.unix_timestamp("reported_at") - F.unix_timestamp(F.lag("reported_at").over(w_all_by_approved_amount))) / 60, 1).alias("minutes_since_prev_reported_at"),
        F.sum("approved_amount").over(w_status_or_unknown).alias("status_or_unknown_sum_approved_amount"),
        F.size(F.collect_set("policy_id").over(w_status_or_unknown)).alias("distinct_policy_id_in_status_or_unknown"),
    )
)

without_matches = (
    ranked.alias("s")
    .join(df_claims.alias("c"), (F.col("c.adjuster_id") == F.col("s.adjuster_id")) & (F.col("c.claim_date") >= F.lit("2024-06-07").cast("date")), "left_semi")
)

without_matches_2 = (
    without_matches.alias("s")
    .join(df_claims.alias("c"), (F.col("c.policy_id") == F.col("s.policy_id")) & (F.col("c.status").isin("APPROVED", "PAID", "OPEN")) & (F.col("c.claimed_amount") <= 44991.5), "left_anti")
    .filter(F.year(F.col("last_claim_date")) == 2025)
)

rollup_base = (
    without_matches_2
    .groupBy("claim_type")
    .agg(
        F.max("status_or_unknown_sum_approved_amount").alias("max_status_or_unknown_sum_approved_amount"),
        F.countDistinct("policy_id").alias("distinct_policy_ids"),
        F.countDistinct("claim_type").alias("distinct_claim_types"),
    )
)

result = (
    rollup_base
    .groupBy("distinct_policy_ids", "distinct_claim_types")
    .pivot("claim_type", ["theft", "delay"])
    .agg(F.max("max_status_or_unknown_sum_approved_amount"))
    .orderBy("distinct_policy_ids", "distinct_claim_types")
)

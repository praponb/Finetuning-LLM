from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_claim_type_or_unknown_adjust = Window.partitionBy("claim_type_or_unknown", "adjuster_id")
w_adjuster_id_by_approved_amou = Window.partitionBy("adjuster_id").orderBy("approved_amount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_adjuster_id_by_approved_amou_2 = Window.partitionBy("adjuster_id").orderBy("approved_amount", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_adjuster_id_by_approved_amou_3 = Window.partitionBy("adjuster_id").orderBy("approved_amount", "id").rowsBetween(-3, Window.currentRow)
w_adjuster_id_by_approved_amou_4 = Window.partitionBy("adjuster_id").orderBy("approved_amount", "id")
w_dedup = Window.partitionBy("claim_policy_id", "policy_holder_id").orderBy("claim_claimed_amount", "id", "claim_id")

scoped_claims = (
    df_claims
    .filter(F.col("claimed_amount") > 12726.7)
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claimed_amount",
        "approved_amount",
        "reported_at",
        F.coalesce(F.col("claim_type"), F.lit("UNKNOWN")).alias("claim_type_or_unknown"),
        F.concat(F.col("status"), F.lit("#"), F.col("adjuster_id").cast("string")).alias("status_ref"),
    )
)

metrics = (
    scoped_claims
    .select(
        "*",
        F.round(F.col("claimed_amount") - F.avg("claimed_amount").over(w_claim_type_or_unknown_adjust), 2).alias("claimed_amount_vs_claim_type_or_unknown_adjuster"),
        F.first("approved_amount", ignorenulls=True).over(w_adjuster_id_by_approved_amou).alias("first_known_approved_amount"),
        F.nth_value("reported_at", 2).over(w_adjuster_id_by_approved_amou_2).alias("second_reported_at"),
        F.max("approved_amount").over(w_adjuster_id_by_approved_amou_3).alias("approved_amount_max_4"),
        F.ntile(10).over(w_adjuster_id_by_approved_amou_4).alias("approved_amount_decile"),
    )
)

joined_policy = (
    metrics.alias("s")
    .join(df_policies.alias("p"), F.col("p.id") == F.col("s.policy_id"), "inner")
    .join(df_adjusters.alias("a"), F.col("a.id") == F.col("s.adjuster_id"), "inner")
    .select(
        "s.id",
        "s.policy_id",
        "s.adjuster_id",
        "s.claimed_amount",
        "s.approved_amount",
        "s.reported_at",
        "s.claim_type_or_unknown",
        "s.status_ref",
        "s.first_known_approved_amount",
        F.col("p.start_date").alias("policy_start_date"),
        F.col("p.holder_id").alias("policy_holder_id"),
        F.col("p.end_date").alias("policy_end_date"),
        F.col("a.level").alias("adjuster_level"),
    )
)

derived = (
    joined_policy
    .select(
        "*",
        F.add_months(F.col("policy_end_date"), 1).alias("policy_end_date_plus_1m"),
        F.substring(F.trim(F.col("claim_type_or_unknown")), 1, 4).alias("claim_type_or_unknown_prefix"),
    )
)

derived_2 = (
    derived
    .select(
        "*",
        F.coalesce(F.col("adjuster_level"), F.lit(0)).alias("adjuster_level_filled"),
        F.datediff(F.lit("2025-12-31").cast("date"), F.col("reported_at")).alias("days_since_reported_at"),
        F.initcap(F.trim(F.col("claim_type_or_unknown"))).alias("claim_type_or_unknown_clean"),
    )
)

exploded_detail_claims = (
    derived_2.alias("s")
    .join(df_claims.alias("c"), (F.col("c.adjuster_id") == F.col("s.adjuster_id")) & (F.col("c.approved_amount") >= 0.0), "inner")
    .select(
        "s.id",
        "s.policy_id",
        "s.adjuster_id",
        "s.approved_amount",
        "s.reported_at",
        "s.status_ref",
        "s.first_known_approved_amount",
        "s.policy_start_date",
        "s.policy_holder_id",
        "s.policy_end_date",
        "s.adjuster_level",
        "s.claim_type_or_unknown_prefix",
        "s.adjuster_level_filled",
        "s.days_since_reported_at",
        "s.claim_type_or_unknown_clean",
        F.col("c.id").alias("claim_id"),
        F.col("c.reported_at").alias("claim_reported_at"),
        F.col("c.approved_amount").alias("claim_approved_amount"),
        F.col("c.claimed_amount").alias("claim_claimed_amount"),
        F.col("c.policy_id").alias("claim_policy_id"),
    )
)

unique_rows = (
    exploded_detail_claims
    .withColumn("recency_rank", F.row_number().over(w_dedup))
    .filter(F.col("recency_rank") == 1)
    .drop("recency_rank")
)

result = (
    unique_rows
    .groupingSets(
        [["adjuster_level", "adjuster_level_filled"], ["adjuster_level"], ["adjuster_level_filled"]],
        "adjuster_level", "adjuster_level_filled",
    )
    .agg(
        F.grouping("adjuster_level").alias("is_total_adjuster_level"),
        F.percentile_approx("days_since_reported_at", 0.5).alias("median_days_since_reported_at"),
    )
    .orderBy(
        F.col("adjuster_level").asc_nulls_last(),
        F.col("adjuster_level_filled").asc_nulls_last(),
    )
)

from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_adjuster_level = Window.orderBy(F.col("adjuster_level").desc(), "id")
w_adjuster_id = Window.partitionBy("adjuster_id")

cleaned_claims = df_claims.filter(~F.col("status").isin("IN_REVIEW", "APPROVED"))

combined_details = (
    cleaned_claims.alias("s")
    .join(df_adjusters.alias("a"), F.col("a.id") == F.col("s.adjuster_id"), "left")
    .join(df_policies.alias("p"), F.col("p.id") == F.col("s.policy_id"), "left")
    .select(
        "s.*",
        F.col("a.level").alias("adjuster_level"),
        "a.adjuster_name",
        F.col("p.start_date").alias("policy_start_date"),
    )
)

windowed = (
    combined_details
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "claim_type",
        "claimed_amount",
        "status",
        "adjuster_level",
        "adjuster_name",
        "policy_start_date",
        F.lag("adjuster_level").over(w_all_by_adjuster_level).alias("prev_adjuster_level"),
        F.min("adjuster_level").over(w_adjuster_id).alias("adjuster_id_min_adjuster_level"),
    )
)

summary = (
    windowed
    .groupBy("prev_adjuster_level")
    .agg(
        F.countDistinct("adjuster_id").alias("distinct_adjuster_ids"),
        F.countDistinct("policy_id").alias("distinct_policy_ids"),
        F.count("*").alias("cnt"),
        F.round(100 * F.count(F.when(F.lower(F.col("adjuster_name")).like("%ar%"), 1)) / F.count("*"), 1).alias("pct_lower_adjuster_name_like_ar"),
        F.count(F.when(~F.col("claim_type").isin("theft", "medical"), 1)).alias("n_claim_type_not_in_theft_medi"),
    )
)

with_activity = (
    summary
    .crossJoin(summary.agg(F.avg("prev_adjuster_level").alias("overall_avg_prev_adjuster_level")))
    .filter(F.col("prev_adjuster_level") > F.col("overall_avg_prev_adjuster_level"))
    .drop("overall_avg_prev_adjuster_level")
)

scored = (
    with_activity
    .select(
        "prev_adjuster_level",
        "distinct_adjuster_ids",
        "distinct_policy_ids",
        "pct_lower_adjuster_name_like_ar",
        "n_claim_type_not_in_theft_medi",
        F.least(F.col("prev_adjuster_level"), F.col("distinct_policy_ids")).alias("least_prev_adjuster_level_distinct_policy_ids"),
        F.abs(F.col("prev_adjuster_level") - 3).alias("prev_adjuster_level_distance"),
        (F.col("distinct_adjuster_ids") - F.col("distinct_policy_ids")).alias("distinct_adjuster_ids_minus_distinct_policy_ids"),
    )
)

result = (
    scored
    .select(
        "prev_adjuster_level",
        "distinct_adjuster_ids",
        "distinct_policy_ids",
        "n_claim_type_not_in_theft_medi",
        "least_prev_adjuster_level_distinct_policy_ids",
        "prev_adjuster_level_distance",
        "distinct_adjuster_ids_minus_distinct_policy_ids",
        F.round(F.col("distinct_policy_ids") / F.nullif(F.col("distinct_adjuster_ids"), F.lit(0)), 3).alias("distinct_policy_ids_per_distinct_adjuster_ids"),
    )
    .orderBy(
        F.col("distinct_adjuster_ids_minus_distinct_policy_ids").desc(),
        "prev_adjuster_level",
    )
    .limit(10)
)

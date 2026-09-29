from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_claim_type_by_total_approved = Window.partitionBy("claim_type").orderBy("total_approved_amount", "status_or_unknown", "claim_type", "policy_id").rowsBetween(-1, Window.currentRow)
w_policy_id = Window.partitionBy("policy_id")
w_claim_type_by_total_approved_2 = Window.partitionBy("claim_type").orderBy("total_approved_amount", "status_or_unknown", "claim_type", "policy_id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

base_claims = (
    df_claims
    .filter(F.col("status").isin("REJECTED", "APPROVED"))
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "claim_type",
        "claimed_amount",
        "approved_amount",
        "reported_at",
        F.coalesce(F.col("status"), F.lit("UNKNOWN")).alias("status_or_unknown"),
        F.round(F.col("claimed_amount") / F.nullif(F.col("approved_amount"), F.lit(0)), 3).alias("claimed_amount_per_approved_amount"),
    )
)

filtered = (
    base_claims
    .crossJoin(base_claims.agg(F.max("claimed_amount").alias("overall_max_claimed_amount")))
    .filter(F.col("claimed_amount") < F.col("overall_max_claimed_amount"))
    .drop("overall_max_claimed_amount")
)

joined_adjuster = (
    filtered.alias("s")
    .join(df_adjusters.alias("a"), F.col("a.id") == F.col("s.adjuster_id"), "left")
    .select(
        "s.id",
        "s.policy_id",
        "s.adjuster_id",
        "s.claim_date",
        "s.claim_type",
        "s.claimed_amount",
        "s.approved_amount",
        "s.status_or_unknown",
        "s.claimed_amount_per_approved_amount",
        "a.adjuster_name",
        F.col("a.team").alias("adjuster_team"),
    )
)

without_matches = (
    joined_adjuster.alias("s")
    .join(df_claims.alias("c"), (F.col("c.policy_id") == F.col("s.policy_id")) & (F.col("c.approved_amount").between(0.0, 17759.3)), "left_anti")
)

policy_id_stats = (
    without_matches
    .groupBy("policy_id")
    .agg(F.avg("approved_amount").alias("avg_approved_amount"))
)

above_average = (
    without_matches.alias("s")
    .join(policy_id_stats.alias("g"), F.col("g.policy_id") == F.col("s.policy_id"), "inner")
    .filter(F.col("s.approved_amount") > F.col("g.avg_approved_amount"))
    .select(
        "s.*",
        "g.avg_approved_amount",
        F.round(F.col("s.approved_amount") - F.col("g.avg_approved_amount"), 2).alias("approved_amount_minus_avg"),
    )
)

rollup_base = (
    above_average
    .groupBy("status_or_unknown", "claim_type", "policy_id")
    .agg(
        F.percentile_approx("avg_approved_amount", 0.5).alias("median_avg_approved_amount"),
        F.array_sort(F.collect_set("claim_type")).alias("claim_type_list"),
        F.array_sort(F.collect_set("adjuster_team")).alias("adjuster_team_list"),
        F.sum("approved_amount").alias("total_approved_amount"),
    )
)

sequenced = (
    rollup_base
    .select(
        "status_or_unknown",
        "claim_type",
        "policy_id",
        "claim_type_list",
        "adjuster_team_list",
        "total_approved_amount",
        F.sum("median_avg_approved_amount").over(w_claim_type_by_total_approved).alias("median_avg_approved_amount_sum_2"),
        F.round(F.avg("median_avg_approved_amount").over(w_policy_id), 2).alias("policy_id_avg_median_avg_approved_amount"),
        F.last("median_avg_approved_amount", ignorenulls=True).over(w_claim_type_by_total_approved_2).alias("last_known_median_avg_approved_amount"),
        F.first("median_avg_approved_amount", ignorenulls=True).over(w_claim_type_by_total_approved_2).alias("first_known_median_avg_approved_amount"),
    )
)

result = (
    sequenced
    .select(
        "status_or_unknown",
        "claim_type",
        "policy_id",
        "claim_type_list",
        "adjuster_team_list",
        "total_approved_amount",
        "median_avg_approved_amount_sum_2",
        "policy_id_avg_median_avg_approved_amount",
        "last_known_median_avg_approved_amount",
    )
    .orderBy("median_avg_approved_amount_sum_2")
)

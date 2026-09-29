from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_claimed_amount = Window.orderBy(F.col("claimed_amount").asc_nulls_last(), "id")
w_policy_id_claim_date_week_mo = Window.partitionBy("policy_id", F.month("claim_date_week"))
w_all_by_claimed_amount_full = Window.orderBy(F.col("claimed_amount").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_claimed_amount_ties = Window.orderBy(F.col("claimed_amount").asc_nulls_last())

scoped_claims = (
    df_claims
    .filter((F.col("reported_at") < F.lit("2025-02-24 12:00:00").cast("timestamp")) & (F.col("adjuster_id") >= 5))
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_type",
        "claimed_amount",
        "approved_amount",
        "status",
        "reported_at",
        F.date_trunc("week", F.col("claim_date")).alias("claim_date_week"),
    )
)

without_matches = (
    scoped_claims
    .crossJoin(scoped_claims.agg(F.avg("approved_amount").alias("overall_avg_approved_amount")))
    .filter(F.col("approved_amount") > F.col("overall_avg_approved_amount"))
    .drop("overall_avg_approved_amount")
)

with_trends = (
    without_matches
    .select(
        "id",
        "policy_id",
        "claim_type",
        "claimed_amount",
        "approved_amount",
        "claim_date_week",
        (F.col("claimed_amount") - F.lag("claimed_amount").over(w_all_by_claimed_amount)).alias("claimed_amount_change"),
        F.min("claimed_amount").over(w_policy_id_claim_date_week_mo).alias("policy_id_claim_date_week_month_min_claimed_amou"),
        F.last("claimed_amount").over(w_all_by_claimed_amount_full).alias("last_claimed_amount"),
        F.count(F.lit(1)).over(w_policy_id_claim_date_week_mo).alias("rows_in_policy_id_claim_date_week_month"),
        F.round(F.avg("approved_amount").over(w_policy_id_claim_date_week_mo), 2).alias("policy_id_claim_date_week_month_avg_approved_amo"),
        F.round(F.percent_rank().over(w_all_by_claimed_amount_ties), 3).alias("claimed_amount_percent_rank"),
    )
)

without_matches_2 = (
    with_trends
    .crossJoin(with_trends.agg(F.avg("claimed_amount_change").alias("overall_avg_claimed_amount_change")))
    .filter(F.col("claimed_amount_change") > F.col("overall_avg_claimed_amount_change"))
    .drop("overall_avg_claimed_amount_change")
)

joined_policy = (
    without_matches_2.alias("s")
    .join(df_policies.alias("p"), F.col("p.id") == F.col("s.policy_id"), "inner")
    .join(df_policyholders.alias("po"), F.col("po.id") == F.col("p.holder_id"), "inner")
    .select(
        "s.id",
        "s.policy_id",
        "s.claim_type",
        "s.claimed_amount",
        "s.approved_amount",
        "s.claim_date_week",
        "s.claimed_amount_change",
        "s.rows_in_policy_id_claim_date_week_month",
        "s.policy_id_claim_date_week_month_avg_approved_amo",
        "s.claimed_amount_percent_rank",
        F.col("p.end_date").alias("policy_end_date"),
        F.col("p.start_date").alias("policy_start_date"),
        F.col("p.holder_id").alias("policy_holder_id"),
        F.col("po.region").alias("holder_region"),
    )
)

policy_stats = (
    df_policies
    .groupBy("holder_id")
    .agg(
        F.count("*").alias("policy_count"),
        F.max("end_date").alias("last_policy_end_date"),
    )
)

profiled = (
    joined_policy.alias("s")
    .join(policy_stats.alias("p"), F.col("p.holder_id") == F.col("s.policy_holder_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("p.policy_count"), F.lit(0)).alias("policy_count"),
        "p.last_policy_end_date",
    )
)

holder_region_stats = (
    profiled
    .groupBy("holder_region")
    .agg(F.percentile_approx("claimed_amount", 0.5).alias("median_claimed_amount"))
)

vs_group = (
    profiled.alias("s")
    .join(holder_region_stats.alias("g"), F.col("g.holder_region") == F.col("s.holder_region"), "inner")
    .filter(F.col("s.claimed_amount") >= F.col("g.median_claimed_amount"))
    .select(
        "s.*",
        "g.median_claimed_amount",
        F.round(F.col("s.claimed_amount") - F.col("g.median_claimed_amount"), 2).alias("claimed_amount_minus_median"),
    )
)

result = (
    vs_group
    .rollup("holder_region", "claim_type", "policy_id")
    .agg(
        F.grouping_id().alias("gid"),
        F.count(F.when(F.col("holder_region") != "Central", 1)).alias("n_holder_region_central"),
        F.count("*").alias("row_count"),
        F.round(F.stddev("claimed_amount_change"), 2).alias("stddev_claimed_amount_change"),
    )
    .select(
        F.coalesce(F.col("holder_region"), F.lit("ALL")).alias("holder_region"),
        F.coalesce(F.col("claim_type"), F.lit("ALL")).alias("claim_type"),
        "policy_id",
        "gid",
        "n_holder_region_central",
        "row_count",
        "stddev_claimed_amount_change",
    )
    .orderBy(
        F.col("holder_region").asc_nulls_last(),
        F.col("claim_type").asc_nulls_last(),
        F.col("policy_id").asc_nulls_last(),
    )
)

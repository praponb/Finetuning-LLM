from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_approved_amount = Window.orderBy(F.col("approved_amount").asc_nulls_last(), "id")
w_all_by_approved_amount_ties = Window.orderBy(F.col("approved_amount").asc_nulls_last())
w_all_by_approved_amount_runni = Window.orderBy(F.col("approved_amount").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_claim_type = Window.partitionBy("claim_type")

recent = df_claims.filter(F.col("policy_id") <= 4)

windowed = (
    recent
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "claim_type",
        "claimed_amount",
        "status",
        "reported_at",
        F.round((F.unix_timestamp("reported_at") - F.unix_timestamp(F.lag("reported_at").over(w_all_by_approved_amount))) / 60, 1).alias("minutes_since_prev_reported_at"),
        F.round(F.cume_dist().over(w_all_by_approved_amount_ties), 3).alias("approved_amount_cume_dist"),
        F.sum("approved_amount").over(w_all_by_approved_amount_runni).alias("running_approved_amount"),
        F.lag("claimed_amount", 1, 0).over(w_all_by_approved_amount).alias("prev_claimed_amount"),
        F.count(F.lit(1)).over(w_claim_type).alias("rows_in_claim_type"),
    )
)

with_details = (
    windowed.alias("s")
    .join(df_adjusters.alias("a"), F.col("a.id") == F.col("s.adjuster_id"), "inner")
    .join(df_policies.alias("p"), F.col("p.id") == F.col("s.policy_id"), "inner")
    .select(
        "s.id",
        "s.policy_id",
        "s.adjuster_id",
        "s.claim_date",
        "s.claim_type",
        "s.status",
        "s.reported_at",
        "s.prev_claimed_amount",
        "s.rows_in_claim_type",
        F.col("a.team").alias("adjuster_team"),
        F.col("a.level").alias("adjuster_level"),
        "a.adjuster_name",
        F.col("p.start_date").alias("policy_start_date"),
        F.col("p.end_date").alias("policy_end_date"),
    )
)

derived = (
    with_details
    .select(
        "*",
        F.least(F.col("prev_claimed_amount"), F.col("adjuster_level")).alias("least_prev_claimed_amount_adjuster_level"),
        F.year(F.col("policy_start_date")).alias("policy_start_date_year"),
    )
)

enriched = (
    derived
    .filter(F.col("policy_end_date").between(F.lit("2025-10-12").cast("date"), F.lit("2026-09-19").cast("date")))
    .select(
        "*",
        F.round(F.col("prev_claimed_amount") * 1.1, 2).alias("prev_claimed_amount_adj"),
        F.quarter(F.col("policy_start_date")).alias("policy_start_date_quarter"),
        F.datediff(F.col("policy_start_date"), F.col("policy_end_date")).alias("days_policy_end_date_to_policy_start_date"),
    )
)

totals = (
    enriched
    .groupBy(
        F.date_trunc("month", "policy_start_date").alias("policy_start_date_month"),
        "policy_start_date_year",
    )
    .agg(
        F.datediff(F.max("policy_start_date"), F.min("policy_start_date")).alias("policy_start_date_span_days"),
        F.percentile_approx("prev_claimed_amount_adj", 0.5).alias("median_prev_claimed_amount_adj"),
        F.round(F.sum("policy_start_date_quarter") / F.nullif(F.sum("adjuster_level"), F.lit(0)), 3).alias("policy_start_date_quarter_to_adjuster_level_rati"),
        F.count("*").alias("row_count"),
    )
)

filtered = (
    totals
    .crossJoin(totals.agg(F.min("policy_start_date_quarter_to_adjuster_level_rati").alias("overall_min_policy_start_date_quarter_to_adjuste")))
    .filter(F.col("policy_start_date_quarter_to_adjuster_level_rati") > F.col("overall_min_policy_start_date_quarter_to_adjuste"))
    .drop("overall_min_policy_start_date_quarter_to_adjuste")
)

result = (
    filtered
    .filter(F.col("policy_start_date_month") < F.lit("2025-02-09 09:00:00").cast("timestamp"))
    .select(
        "policy_start_date_month",
        "policy_start_date_year",
        "policy_start_date_quarter_to_adjuster_level_rati",
        "row_count",
    )
    .orderBy(
        F.col("row_count").desc(),
        "policy_start_date_quarter_to_adjuster_level_rati",
    )
)

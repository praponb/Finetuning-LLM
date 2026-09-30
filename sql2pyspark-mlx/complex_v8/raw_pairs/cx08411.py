from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_status_adjuster_id_by_claim = Window.partitionBy("status", "adjuster_id").orderBy(F.col("claim_date").asc_nulls_last(), "id", "claim_id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_status_adjuster_id_by_claim_2 = Window.partitionBy("status", "adjuster_id").orderBy(F.col("claim_date").asc_nulls_last())
w_claim_date_month_policy_id_b = Window.partitionBy(F.date_trunc("month", "claim_date"), "policy_id").orderBy("approved_amount", "id", "claim_id")
w_status = Window.partitionBy("status")

recent_claims = (
    df_claims
    .filter((F.col("claim_date").between(F.lit("2024-04-11").cast("date"), F.lit("2025-08-25").cast("date"))) & (F.col("adjuster_id") <= 4))
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

detail_rows = (
    recent_claims.alias("s")
    .join(df_claims.alias("c"), (F.col("c.adjuster_id") == F.col("s.adjuster_id")) & (F.col("c.claim_date") < F.lit("2024-08-20").cast("date")), "inner")
    .select(
        "s.id",
        "s.policy_id",
        "s.adjuster_id",
        "s.claimed_amount",
        "s.approved_amount",
        "s.status",
        "s.claim_date_week",
        F.col("c.id").alias("claim_id"),
        "c.claim_date",
        F.col("c.reported_at").alias("claim_reported_at"),
    )
)

windowed = (
    detail_rows
    .select(
        "*",
        F.nth_value("claim_date_week", 2).over(w_status_adjuster_id_by_claim).alias("second_claim_date_week"),
        F.rank().over(w_status_adjuster_id_by_claim_2).alias("claim_date_rank"),
        F.ntile(4).over(w_claim_date_month_policy_id_b).alias("approved_amount_quartile"),
        F.max("approved_amount").over(w_status).alias("status_max_approved_amount"),
        F.row_number().over(w_claim_date_month_policy_id_b).alias("seq_no"),
        F.rank().over(w_claim_date_month_policy_id_b).alias("approved_amount_rank"),
    )
)

enriched = (
    windowed.alias("s")
    .join(df_policies.alias("p"), F.col("p.id") == F.col("s.policy_id"), "left")
    .select(
        "s.id",
        "s.policy_id",
        "s.adjuster_id",
        "s.claimed_amount",
        "s.approved_amount",
        "s.status",
        "s.claim_id",
        "s.claim_date",
        "s.second_claim_date_week",
        "s.approved_amount_quartile",
        "s.status_max_approved_amount",
        "s.seq_no",
        F.col("p.end_date").alias("policy_end_date"),
        F.col("p.start_date").alias("policy_start_date"),
    )
)

line_level_claims = (
    enriched.alias("s")
    .join(df_claims.alias("c"), F.col("c.policy_id") == F.col("s.policy_id"), "inner")
    .select(
        "s.id",
        "s.policy_id",
        "s.adjuster_id",
        "s.claimed_amount",
        "s.approved_amount",
        "s.claim_id",
        "s.claim_date",
        "s.second_claim_date_week",
        "s.approved_amount_quartile",
        "s.status_max_approved_amount",
        "s.policy_start_date",
        F.col("c.id").alias("claim_id_2"),
        F.col("c.approved_amount").alias("claim_approved_amount"),
        F.col("c.claim_date").alias("claim_claim_date"),
        F.col("c.adjuster_id").alias("claim_adjuster_id"),
    )
)

adjuster_id_stats = (
    line_level_claims
    .groupBy("adjuster_id")
    .agg(F.max("status_max_approved_amount").alias("max_status_max_approved_amount"))
)

outliers = (
    line_level_claims.alias("s")
    .join(adjuster_id_stats.alias("g"), F.col("g.adjuster_id") == F.col("s.adjuster_id"), "inner")
    .filter(F.col("s.status_max_approved_amount") == F.col("g.max_status_max_approved_amount"))
    .select(
        "s.*",
        "g.max_status_max_approved_amount",
        F.round(F.col("s.status_max_approved_amount") - F.col("g.max_status_max_approved_amount"), 2).alias("status_max_approved_amount_minus_max"),
    )
)

claim_stats = (
    df_claims
    .groupBy("adjuster_id")
    .agg(F.max("reported_at").alias("last_claim_reported_at"))
)

profiled = (
    outliers.alias("s")
    .join(claim_stats.alias("c"), F.col("c.adjuster_id") == F.col("s.adjuster_id"), "left")
    .select("s.*", "c.last_claim_reported_at")
)

result = (
    profiled
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claimed_amount",
        "approved_amount",
        "claim_id",
        "second_claim_date_week",
        "approved_amount_quartile",
        "status_max_approved_amount",
        "claim_id_2",
        "claim_approved_amount",
        "claim_claim_date",
        "claim_adjuster_id",
        "max_status_max_approved_amount",
        "status_max_approved_amount_minus_max",
    )
    .orderBy(F.col("status_max_approved_amount_minus_max").desc_nulls_first())
)

from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_claim_type = Window.partitionBy("claim_type")
w_policy_id_adjuster_team_by_c = Window.partitionBy("policy_id", "adjuster_team").orderBy(F.col("claimed_amount").desc(), "id").rowsBetween(-3, Window.currentRow)

cleaned_claims = (
    df_claims
    .filter(F.col("claim_date").between(F.lit("2024-05-14").cast("date"), F.lit("2025-04-25").cast("date")))
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "claim_type",
        "claimed_amount",
        "approved_amount",
        "reported_at",
        F.last_day(F.col("claim_date")).alias("claim_date_month_end"),
        F.least(F.col("approved_amount"), F.col("claimed_amount")).alias("least_approved_amount_claimed_amount"),
        F.month(F.col("claim_date")).alias("claim_date_month"),
    )
)

labeled = (
    cleaned_claims
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "claim_type",
        "claimed_amount",
        "approved_amount",
        "reported_at",
        "least_approved_amount_claimed_amount",
        (F.col("approved_amount") - F.col("claim_date_month")).alias("approved_amount_minus_claim_date_month"),
        F.substring(F.trim(F.col("claim_type")), 1, 2).alias("claim_type_prefix"),
        F.when(F.col("claimed_amount") >= 29336.0, "HIGH").when(F.col("claimed_amount") >= 14019.8, "MEDIUM").otherwise("LOW").alias("claimed_amount_tier"),
    )
)

enriched_adjuster = (
    labeled.alias("s")
    .join(df_adjusters.alias("a"), F.col("a.id") == F.col("s.adjuster_id"), "inner")
    .select("s.*", F.col("a.team").alias("adjuster_team"))
)

with_activity = (
    enriched_adjuster
    .crossJoin(enriched_adjuster.agg(F.avg("approved_amount_minus_claim_date_month").alias("overall_avg_approved_amount_minus_claim_date_mon")))
    .filter(F.col("approved_amount_minus_claim_date_month") > F.col("overall_avg_approved_amount_minus_claim_date_mon"))
    .drop("overall_avg_approved_amount_minus_claim_date_mon")
)

windowed = (
    with_activity
    .select(
        "*",
        F.size(F.collect_set("adjuster_id").over(w_claim_type)).alias("distinct_adjuster_id_in_claim_type"),
        F.max("claimed_amount").over(w_policy_id_adjuster_team_by_c).alias("claimed_amount_max_4"),
    )
)

with_parent_adjuster = (
    windowed.alias("s")
    .join(df_adjusters.alias("a"), F.col("a.id") == F.col("s.adjuster_id"), "inner")
    .select(
        "s.id",
        "s.policy_id",
        "s.adjuster_id",
        "s.claim_date",
        "s.claimed_amount",
        "s.approved_amount",
        "s.reported_at",
        "s.least_approved_amount_claimed_amount",
        "s.approved_amount_minus_claim_date_month",
        "s.claimed_amount_tier",
        "s.adjuster_team",
        "s.distinct_adjuster_id_in_claim_type",
        "s.claimed_amount_max_4",
        F.col("a.team").alias("adjuster_team_2"),
    )
)

result = (
    with_parent_adjuster
    .filter(F.col("adjuster_team_2") == "Travel")
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "approved_amount",
        "reported_at",
        "approved_amount_minus_claim_date_month",
        "adjuster_team",
        "distinct_adjuster_id_in_claim_type",
        "claimed_amount_max_4",
        F.when(F.col("claimed_amount") >= 41760.8, "HIGH").when(F.col("claimed_amount") >= 19609.8, "MEDIUM").otherwise("LOW").alias("claimed_amount_tier_2"),
        F.add_months(F.col("claim_date"), 6).alias("claim_date_plus_6m"),
    )
    .orderBy(F.col("reported_at").desc(), F.col("id").asc_nulls_last())
)

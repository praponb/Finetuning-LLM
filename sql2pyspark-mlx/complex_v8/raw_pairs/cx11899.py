from pyspark.sql import functions as F

valid = (
    df_claims
    .filter(F.col("approved_amount") > 0.0)
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "claimed_amount",
        "approved_amount",
        "status",
        "reported_at",
        F.last_day(F.col("claim_date")).alias("claim_date_month_end"),
        F.coalesce(F.col("claimed_amount"), F.lit(0.0)).alias("claimed_amount_filled"),
    )
)

with_parent_adjuster = (
    valid.alias("s")
    .join(df_adjusters.alias("a"), F.col("a.id") == F.col("s.adjuster_id"), "left")
    .select(
        "s.id",
        "s.policy_id",
        "s.adjuster_id",
        "s.claim_date",
        "s.claimed_amount",
        "s.status",
        "s.claim_date_month_end",
        "s.claimed_amount_filled",
        "a.adjuster_name",
        F.col("a.team").alias("adjuster_team"),
    )
)

derived = (
    with_parent_adjuster
    .select(
        "*",
        F.abs(F.col("claimed_amount_filled") - 15070.0).alias("claimed_amount_filled_distance"),
        F.greatest(F.col("claimed_amount"), F.col("claimed_amount_filled")).alias("greatest_claimed_amount_claimed_amount_filled"),
        F.substring(F.trim(F.col("status")), 1, 3).alias("status_prefix"),
    )
)

result = (
    derived
    .groupBy("adjuster_id", "claim_date")
    .pivot("adjuster_team", ["Health", "Travel", "Property", "Motor"])
    .agg(F.max("claimed_amount_filled_distance").alias("max_v"), F.sum("claimed_amount_filled_distance").alias("sum_v"))
)

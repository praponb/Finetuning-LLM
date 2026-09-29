from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_latest = Window.partitionBy("adjuster_id").orderBy(F.col("approved_amount_distance").desc(), F.col("id").desc())

base = (
    df_claims
    .filter((F.col("adjuster_id") >= 6) & (F.col("policy_id") >= 27))
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "claim_type",
        "claimed_amount",
        "reported_at",
        F.datediff(F.lit("2025-06-30").cast("date"), F.col("claim_date")).alias("days_since_claim_date"),
        F.abs(F.col("approved_amount") - 10355.0).alias("approved_amount_distance"),
        (F.col("claimed_amount") + F.col("approved_amount")).alias("claimed_amount_plus_approved_amount"),
    )
)

scored = (
    base
    .select(
        "id",
        "adjuster_id",
        "claimed_amount",
        "days_since_claim_date",
        "approved_amount_distance",
        "claimed_amount_plus_approved_amount",
        F.least(F.col("approved_amount_distance"), F.col("days_since_claim_date")).alias("least_approved_amount_distance_days_since_claim_"),
        F.substring(F.trim(F.col("claim_type")), 1, 2).alias("claim_type_prefix"),
    )
)

latest = (
    scored
    .withColumn("recency_rank", F.row_number().over(w_latest))
    .filter(F.col("recency_rank") == 1)
    .drop("recency_rank")
)

adjuster_id_stats = (
    latest
    .groupBy("adjuster_id")
    .agg(F.avg("claimed_amount").alias("avg_claimed_amount"))
)

above_average = (
    latest.alias("s")
    .join(adjuster_id_stats.alias("g"), F.col("g.adjuster_id") == F.col("s.adjuster_id"), "inner")
    .filter(F.col("s.claimed_amount") > F.col("g.avg_claimed_amount"))
    .select(
        "s.*",
        "g.avg_claimed_amount",
        F.round(F.col("s.claimed_amount") - F.col("g.avg_claimed_amount"), 2).alias("claimed_amount_minus_avg"),
    )
)

exploded_detail = (
    above_average.alias("s")
    .join(df_claims.alias("c"), F.col("c.adjuster_id") == F.col("s.adjuster_id"), "inner")
    .select(
        "s.id",
        "s.adjuster_id",
        "s.days_since_claim_date",
        "s.approved_amount_distance",
        "s.claimed_amount_plus_approved_amount",
        "s.least_approved_amount_distance_days_since_claim_",
        "s.avg_claimed_amount",
        "s.claimed_amount_minus_avg",
        F.col("c.id").alias("claim_id"),
        F.col("c.policy_id").alias("claim_policy_id"),
        "c.claim_type",
    )
)

scored_2 = (
    exploded_detail
    .select(
        "id",
        "days_since_claim_date",
        "approved_amount_distance",
        "claimed_amount_plus_approved_amount",
        "least_approved_amount_distance_days_since_claim_",
        "claimed_amount_minus_avg",
        "claim_id",
        "claim_policy_id",
        F.coalesce(F.col("claim_type"), F.lit("UNKNOWN")).alias("claim_type_or_unknown"),
        F.lower(F.regexp_replace(F.trim(F.col("claim_type")), " ", "_")).alias("claim_type_slug"),
        F.abs(F.col("claimed_amount_minus_avg") - -10000.0).alias("claimed_amount_minus_avg_distance"),
    )
)

joined = (
    scored_2.alias("s")
    .join(df_policies.alias("p"), F.col("p.id") == F.col("s.claim_policy_id"), "inner")
    .join(df_claims.alias("c"), F.col("c.id") == F.col("s.claim_id"), "left")
    .select(
        "s.id",
        "s.approved_amount_distance",
        "s.claimed_amount_plus_approved_amount",
        "s.least_approved_amount_distance_days_since_claim_",
        "s.claimed_amount_minus_avg",
        "s.claim_id",
        "s.claim_policy_id",
        "s.claim_type_or_unknown",
        "s.claimed_amount_minus_avg_distance",
        F.col("p.start_date").alias("policy_start_date"),
        F.col("p.premium").alias("policy_premium"),
        F.col("p.product").alias("policy_product"),
        F.col("c.status").alias("claim_status"),
    )
)

result = (
    joined
    .select(
        "id",
        "approved_amount_distance",
        "claimed_amount_plus_approved_amount",
        "least_approved_amount_distance_days_since_claim_",
        "claimed_amount_minus_avg",
        "claim_type_or_unknown",
        "claimed_amount_minus_avg_distance",
        "policy_start_date",
        F.concat(F.col("claim_status"), F.lit("#"), F.col("id").cast("string")).alias("claim_status_ref"),
    )
    .orderBy(F.col("claimed_amount_minus_avg_distance").asc())
)

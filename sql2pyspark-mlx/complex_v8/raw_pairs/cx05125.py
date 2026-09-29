from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_policy_id_by_avg_claim_appro = Window.partitionBy("policy_id").orderBy("avg_claim_approved_amount", "id", "claim_id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_policy_id_by_avg_claim_appro_2 = Window.partitionBy("policy_id").orderBy("avg_claim_approved_amount")
w_claim_policy_id = Window.partitionBy("claim_policy_id")

filtered_claims = (
    df_claims
    .filter(F.col("reported_at") < F.lit("2025-02-12 12:00:00").cast("timestamp"))
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "claim_type",
        "claimed_amount",
        "reported_at",
        (F.col("claimed_amount") - F.col("approved_amount")).alias("claimed_amount_minus_approved_amount"),
    )
)

claim_stats = (
    df_claims
    .groupBy("adjuster_id")
    .agg(F.max("reported_at").alias("last_claim_reported_at"))
)

with_stats = (
    filtered_claims.alias("s")
    .join(claim_stats.alias("c"), F.col("c.adjuster_id") == F.col("s.adjuster_id"), "left")
    .select("s.*", "c.last_claim_reported_at")
)

line_level_claims = (
    with_stats.alias("s")
    .join(df_claims.alias("c"), F.col("c.adjuster_id") == F.col("s.adjuster_id"), "left")
    .select(
        "s.id",
        "s.policy_id",
        "s.adjuster_id",
        "s.claim_type",
        "s.claimed_amount",
        "s.reported_at",
        "s.claimed_amount_minus_approved_amount",
        "s.last_claim_reported_at",
        F.col("c.id").alias("claim_id"),
        F.col("c.policy_id").alias("claim_policy_id"),
        F.col("c.approved_amount").alias("claim_approved_amount"),
    )
)

claim_type_stats = (
    line_level_claims
    .groupBy("claim_type")
    .agg(F.avg("claim_approved_amount").alias("avg_claim_approved_amount"))
)

above_average = (
    line_level_claims.alias("s")
    .join(claim_type_stats.alias("g"), F.col("g.claim_type") == F.col("s.claim_type"), "inner")
    .filter(F.col("s.claim_approved_amount") > F.col("g.avg_claim_approved_amount"))
    .select(
        "s.*",
        "g.avg_claim_approved_amount",
        F.round(F.col("s.claim_approved_amount") - F.col("g.avg_claim_approved_amount"), 2).alias("claim_approved_amount_minus_avg"),
    )
)

windowed = (
    above_average
    .select(
        "id",
        "adjuster_id",
        "claimed_amount",
        "last_claim_reported_at",
        "claim_id",
        "avg_claim_approved_amount",
        F.last("claim_approved_amount").over(w_policy_id_by_avg_claim_appro).alias("last_claim_approved_amount"),
        F.dense_rank().over(w_policy_id_by_avg_claim_appro_2).alias("avg_claim_approved_amount_dense_rank"),
        F.round(F.col("claim_approved_amount") - F.avg("claim_approved_amount").over(w_claim_policy_id), 2).alias("claim_approved_amount_vs_claim_policy_id_avg"),
        F.count(F.lit(1)).over(w_claim_policy_id).alias("rows_in_claim_policy_id"),
    )
)

result = (
    windowed
    .filter(F.col("last_claim_approved_amount") <= 16404.6)
    .select("rows_in_claim_policy_id")
    .distinct()
    .orderBy(F.col("rows_in_claim_policy_id").desc())
)

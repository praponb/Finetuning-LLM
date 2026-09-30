from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_status_by_claim_date_plus_90 = Window.partitionBy("status").orderBy(F.col("claim_date_plus_90d").desc(), "id")
w_policy_id_claim_type_len_by = Window.partitionBy("policy_id", "claim_type_len").orderBy("claim_date", "id").rowsBetween(-6, -1)
w_status = Window.partitionBy("status")

valid_claims = (
    df_claims
    .filter(F.col("approved_amount").between(0.0, 23487.5))
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "claim_type",
        "status",
        F.when(F.col("approved_amount") >= 14377.6, "HIGH").when(F.col("approved_amount") >= 1364.5, "MEDIUM").otherwise("LOW").alias("approved_amount_tier"),
        F.length(F.trim(F.col("claim_type"))).alias("claim_type_len"),
    )
)

with_parent_adjuster = (
    valid_claims.alias("s")
    .join(df_adjusters.alias("a"), F.col("a.id") == F.col("s.adjuster_id"), "inner")
    .select(
        "s.id",
        "s.policy_id",
        "s.adjuster_id",
        "s.claim_date",
        "s.claim_type",
        "s.status",
        "s.approved_amount_tier",
        "s.claim_type_len",
        F.col("a.team").alias("adjuster_team"),
    )
)

enriched = (
    with_parent_adjuster
    .filter(F.col("claim_type_len") <= 11)
    .select(
        "*",
        F.abs(F.col("claim_type_len") - 9).alias("claim_type_len_distance"),
        F.date_add(F.col("claim_date"), 90).alias("claim_date_plus_90d"),
        F.concat(F.col("claim_type"), F.lit("#"), F.col("adjuster_id").cast("string")).alias("claim_type_ref"),
    )
)

sequenced = (
    enriched
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "claim_type_len",
        "adjuster_team",
        F.lag("claim_date").over(w_status_by_claim_date_plus_90).alias("prev_claim_date"),
        F.max("claim_type_len").over(w_policy_id_claim_type_len_by).alias("claim_type_len_max_6_prior"),
        F.round(F.col("claim_type_len_distance") / F.nullif(F.sum("claim_type_len_distance").over(w_status), F.lit(0)), 4).alias("claim_type_len_distance_share_of_status"),
        F.round(F.col("claim_type_len") / F.nullif(F.sum("claim_type_len").over(w_status), F.lit(0)), 4).alias("claim_type_len_share_of_status"),
    )
)

claim_stats = (
    df_claims
    .groupBy("adjuster_id")
    .agg(
        F.count(F.when(F.col("status") == "OPEN", 1)).alias("claim_status_count"),
        F.min("claimed_amount").alias("min_claim_claimed_amount"),
        F.max("claim_date").alias("last_claim_date"),
    )
)

summarized_rows = (
    sequenced.alias("s")
    .join(claim_stats.alias("c"), F.col("c.adjuster_id") == F.col("s.adjuster_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("c.claim_status_count"), F.lit(0)).alias("claim_status_count"),
        "c.min_claim_claimed_amount",
        "c.last_claim_date",
    )
)

result = (
    summarized_rows
    .select(
        "id",
        "policy_id",
        "claim_date",
        "claim_type_len",
        "adjuster_team",
        "prev_claim_date",
        "claim_type_len_max_6_prior",
        "claim_type_len_distance_share_of_status",
        "min_claim_claimed_amount",
        "last_claim_date",
    )
    .orderBy(F.col("prev_claim_date").desc(), "adjuster_team")
)

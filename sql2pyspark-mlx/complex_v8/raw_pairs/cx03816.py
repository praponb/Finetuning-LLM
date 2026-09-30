from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_dedup = Window.partitionBy("claimed_amount_tier", "status").orderBy(F.col("claim_date").desc(), F.col("id").desc())

prepared = (
    df_claims
    .filter((F.month(F.col("reported_at")).isin(2, 3)) & (F.col("adjuster_id") <= 2))
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "claim_type",
        "claimed_amount",
        "approved_amount",
        "status",
        "reported_at",
        F.when(F.col("claimed_amount") >= 34401.0, "large").when(F.col("claimed_amount") >= 7204.1, "medium").otherwise("small").alias("claimed_amount_tier"),
        F.date_add(F.col("claim_date"), 30).alias("claim_date_plus_30d"),
        (F.floor(F.col("claimed_amount") / 10000) * 10000).alias("claimed_amount_bucket"),
    )
)

adjuster_id_stats = (
    prepared
    .groupBy("adjuster_id")
    .agg(F.percentile_approx("claimed_amount", 0.5).alias("median_claimed_amount"))
)

above_average = (
    prepared.alias("s")
    .join(adjuster_id_stats.alias("g"), F.col("g.adjuster_id") == F.col("s.adjuster_id"), "inner")
    .filter(F.col("s.claimed_amount") >= F.col("g.median_claimed_amount"))
    .select(
        "s.*",
        "g.median_claimed_amount",
        F.round(F.col("s.claimed_amount") - F.col("g.median_claimed_amount"), 2).alias("claimed_amount_minus_median"),
    )
)

combined_details = (
    above_average.alias("s")
    .join(df_adjusters.alias("a"), F.col("a.id") == F.col("s.adjuster_id"), "inner")
    .join(df_policies.alias("p"), F.col("p.id") == F.col("s.policy_id"), "inner")
    .filter(F.upper(F.trim(F.col("a.adjuster_name"))).like("H%"))
    .select(
        "s.id",
        "s.policy_id",
        "s.adjuster_id",
        "s.claim_date",
        "s.claim_type",
        "s.claimed_amount",
        "s.approved_amount",
        "s.status",
        "s.reported_at",
        "s.claimed_amount_tier",
        "s.claim_date_plus_30d",
        "s.claimed_amount_minus_median",
        "a.adjuster_name",
        F.col("a.level").alias("adjuster_level"),
        F.col("p.premium").alias("policy_premium"),
        F.col("p.end_date").alias("policy_end_date"),
    )
)

latest = (
    combined_details
    .withColumn("row_num", F.row_number().over(w_dedup))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

result = (
    latest
    .cube("status", "adjuster_level")
    .agg(
        F.sum(F.when(F.col("claim_type") != "fire", F.col("policy_premium"))).alias("policy_premium_claim_type_fire"),
        F.round(F.sum("claimed_amount") / F.nullif(F.sum("adjuster_level"), F.lit(0)), 3).alias("claimed_amount_to_adjuster_level_ratio"),
    )
    .select(
        F.coalesce(F.col("status"), F.lit("ALL")).alias("status"),
        "adjuster_level",
        "policy_premium_claim_type_fire",
        "claimed_amount_to_adjuster_level_ratio",
    )
    .orderBy(
        F.col("status").asc_nulls_last(),
        F.col("adjuster_level").asc_nulls_last(),
    )
)

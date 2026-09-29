from pyspark.sql import functions as F

filtered = (
    df_claims
    .filter((F.col("claim_date") < F.lit("2024-12-20").cast("date")) & (F.col("reported_at") >= F.lit("2025-02-06 12:00:00").cast("timestamp")))
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "claim_type",
        "status",
        "reported_at",
        (F.col("reported_at") + F.expr("INTERVAL 15 MINUTES")).alias("reported_at_plus_15m"),
        F.date_format(F.col("claim_date"), "yyyy-MM-dd").alias("claim_date_label"),
    )
)

qualified = (
    filtered.alias("s")
    .join(df_claims.alias("c"), F.col("c.policy_id") == F.col("s.policy_id"), "left_semi")
)

combined_details = (
    qualified.alias("s")
    .join(df_policies.alias("p"), F.col("p.id") == F.col("s.policy_id"), "left")
    .select("s.*", F.col("p.status").alias("policy_status"))
)

enriched_adjuster = (
    combined_details.alias("s")
    .join(df_adjusters.alias("a"), (F.col("a.id") == F.col("s.adjuster_id")) & (F.upper(F.trim(F.col("a.adjuster_name"))).like("G%")), "inner")
    .join(df_policies.alias("p"), F.col("p.id") == F.col("s.policy_id"), "inner")
    .select(
        "s.id",
        "s.policy_id",
        "s.adjuster_id",
        "s.claim_date",
        "s.reported_at_plus_15m",
        "s.claim_date_label",
        "s.policy_status",
        F.col("a.level").alias("adjuster_level"),
        F.col("a.team").alias("adjuster_team"),
        "a.adjuster_name",
        F.col("p.premium").alias("policy_premium"),
    )
)

result = (
    enriched_adjuster
    .select(
        "id",
        "adjuster_id",
        "claim_date",
        "reported_at_plus_15m",
        "claim_date_label",
        "policy_status",
        "adjuster_level",
        "adjuster_name",
        "policy_premium",
        F.lower(F.regexp_replace(F.trim(F.col("policy_status")), " ", "_")).alias("policy_status_slug"),
    )
    .orderBy(F.col("adjuster_level").asc_nulls_last(), F.col("id").desc())
)

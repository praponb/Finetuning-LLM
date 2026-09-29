from pyspark.sql import functions as F

recent_policys = (
    df_policies
    .filter(F.col("start_date") >= F.lit("2024-06-19").cast("date"))
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "start_date",
        F.lower(F.regexp_replace(F.trim(F.col("status")), " ", "_")).alias("status_slug"),
    )
)

claim_stats = (
    df_claims
    .groupBy("policy_id")
    .agg(F.count("*").alias("claim_count"))
)

with_counts = (
    recent_policys.alias("s")
    .join(claim_stats.alias("c"), F.col("c.policy_id") == F.col("s.id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("c.claim_count"), F.lit(0)).alias("claim_count"),
    )
)

with_details = (
    with_counts.alias("s")
    .join(df_policyholders.alias("p"), F.col("p.id") == F.col("s.holder_id"), "inner")
    .select(
        "s.id",
        "s.holder_id",
        "s.premium",
        "s.start_date",
        "s.status_slug",
        F.col("p.risk_score").alias("holder_risk_score"),
    )
)

with_parent_holder = (
    with_details.alias("s")
    .join(df_policyholders.alias("p"), F.col("p.id") == F.col("s.holder_id"), "inner")
    .select(
        "s.id",
        "s.holder_id",
        "s.premium",
        "s.start_date",
        "s.holder_risk_score",
        F.col("p.risk_score").alias("holder_risk_score_2"),
        "p.holder_name",
    )
)

result = (
    with_parent_holder
    .select(
        "id",
        "premium",
        "start_date",
        "holder_risk_score",
        "holder_risk_score_2",
        "holder_name",
        F.substring(F.trim(F.col("holder_name")), 1, 3).alias("holder_name_prefix"),
    )
    .orderBy(F.col("holder_risk_score_2").asc_nulls_last(), "premium")
)

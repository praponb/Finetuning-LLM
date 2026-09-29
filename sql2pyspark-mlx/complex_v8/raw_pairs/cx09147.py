from pyspark.sql import functions as F

base = (
    df_policies
    .filter(F.col("holder_id").isNotNull())
    .select(
        "id",
        "holder_id",
        "product",
        "start_date",
        "status",
        F.coalesce(F.col("status"), F.lit("UNKNOWN")).alias("status_or_unknown"),
        F.coalesce(F.col("premium"), F.lit(0.0)).alias("premium_filled"),
        F.when(F.col("status").isin("EXPIRED", "LAPSED"), "group_a").when(F.col("status") == "ACTIVE", "group_b").otherwise("other").alias("status_group"),
    )
)

combined_details = (
    base.alias("s")
    .join(df_policyholders.alias("p"), F.col("p.id") == F.col("s.holder_id"), "inner")
    .select(
        "s.id",
        "s.holder_id",
        "s.product",
        "s.start_date",
        "s.status",
        "s.status_or_unknown",
        "s.premium_filled",
        "s.status_group",
        F.col("p.joined_on").alias("holder_joined_on"),
        F.col("p.risk_score").alias("holder_risk_score"),
    )
)

with_activity = (
    combined_details.alias("s")
    .join(df_claims.alias("c"), F.col("c.policy_id") == F.col("s.id"), "left_semi")
)

enriched_holder = (
    with_activity.alias("s")
    .join(df_policyholders.alias("p"), F.col("p.id") == F.col("s.holder_id"), "inner")
    .select(
        "s.id",
        "s.holder_id",
        "s.product",
        "s.start_date",
        "s.status",
        "s.status_group",
        "s.holder_joined_on",
        "s.holder_risk_score",
        F.col("p.risk_score").alias("holder_risk_score_2"),
        F.col("p.segment").alias("holder_segment"),
        F.col("p.region").alias("holder_region"),
    )
)

result = (
    enriched_holder
    .filter(F.col("holder_segment") == "RETAIL")
    .select(
        "id",
        "holder_id",
        "product",
        "status",
        "status_group",
        "holder_joined_on",
        "holder_risk_score",
        "holder_risk_score_2",
        "holder_segment",
        "holder_region",
        F.datediff(F.lit("2026-01-01").cast("date"), F.col("holder_joined_on")).alias("days_since_holder_joined_on"),
    )
    .orderBy(F.col("holder_segment").asc())
)

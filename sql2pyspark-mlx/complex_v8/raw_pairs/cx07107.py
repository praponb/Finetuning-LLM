from pyspark.sql import functions as F

recent = (
    df_policies
    .filter(F.col("start_date").between(F.lit("2024-04-03").cast("date"), F.lit("2025-10-13").cast("date")))
)

exploded_detail_claims = (
    recent.alias("s")
    .join(df_claims.alias("c"), F.col("c.policy_id") == F.col("s.id"), "left")
    .select(
        "s.id",
        "s.holder_id",
        "s.premium",
        "s.start_date",
        "s.end_date",
        "s.status",
        F.col("c.id").alias("claim_id"),
        "c.claim_type",
        F.col("c.adjuster_id").alias("claim_adjuster_id"),
    )
)

with_parent = (
    exploded_detail_claims.alias("s")
    .join(df_claims.alias("c"), F.col("c.id") == F.col("s.claim_id"), "inner")
    .select(
        "s.id",
        "s.holder_id",
        "s.end_date",
        "s.status",
        "s.claim_id",
        "s.claim_type",
        "s.claim_adjuster_id",
        F.col("c.status").alias("claim_status"),
    )
)

annotated = (
    with_parent
    .select(
        "id",
        "holder_id",
        "end_date",
        "status",
        "claim_id",
        "claim_type",
        "claim_status",
        F.date_add(F.col("end_date"), 30).alias("end_date_plus_30d"),
    )
)

result = (
    annotated
    .filter(F.col("claim_id").isNotNull())
    .select(
        "id",
        "status",
        "claim_type",
        "end_date_plus_30d",
        F.concat(F.col("status"), F.lit("#"), F.col("holder_id").cast("string")).alias("status_ref"),
    )
    .orderBy(F.col("end_date_plus_30d").desc())
)

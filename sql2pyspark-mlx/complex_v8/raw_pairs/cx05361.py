from pyspark.sql import functions as F

prepared_adjusters = (
    df_adjusters
    .filter((F.col("team").isNotNull()) & (F.col("level") >= 3))
    .select(
        "id",
        "adjuster_name",
        "team",
        "level",
        F.coalesce(F.col("team"), F.lit("UNKNOWN")).alias("team_or_unknown"),
        F.concat(F.col("team"), F.lit("#"), F.col("id").cast("string")).alias("team_ref"),
    )
)

claim_stats = (
    df_claims
    .groupBy("adjuster_id")
    .agg(F.min("reported_at").alias("first_claim_reported_at"))
)

with_counts = (
    prepared_adjusters.alias("s")
    .join(claim_stats.alias("c"), F.col("c.adjuster_id") == F.col("s.id"), "left")
    .select("s.*", "c.first_claim_reported_at")
)

result = (
    with_counts
    .cube("level", "team", "team_or_unknown")
    .agg(
        F.grouping("level").alias("is_total_level"),
        F.count(F.when(F.col("adjuster_name").isNotNull(), 1)).alias("n_adjuster_name_is_not_null"),
    )
    .orderBy(
        F.col("level").asc_nulls_last(),
        F.col("team").asc_nulls_last(),
        F.col("team_or_unknown").asc_nulls_last(),
    )
)

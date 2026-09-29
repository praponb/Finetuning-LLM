from pyspark.sql import functions as F

cleaned = (
    df_adjusters
    .filter((F.col("level") >= 3) & (F.col("team").isNotNull()))
    .select(
        "id",
        "adjuster_name",
        "team",
        "level",
    )
)

enriched = (
    cleaned
    .select(
        "id",
        "adjuster_name",
        "team",
        "level",
        F.lower(F.regexp_replace(F.trim(F.col("adjuster_name")), " ", "_")).alias("adjuster_name_slug"),
        F.when(F.col("team").isin("Health", "Property"), "group_a").when(F.col("team") == "Motor", "group_b").otherwise("other").alias("team_group"),
    )
)

claim_stats = (
    df_claims
    .groupBy("adjuster_id")
    .agg(
        F.count("*").alias("claim_count"),
        F.max("claim_date").alias("last_claim_date"),
    )
)

with_stats = (
    enriched.alias("s")
    .join(claim_stats.alias("c"), F.col("c.adjuster_id") == F.col("s.id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("c.claim_count"), F.lit(0)).alias("claim_count"),
        "c.last_claim_date",
    )
)

labeled = (
    with_stats
    .withColumn("least_claim_count_level", F.least(F.col("claim_count"), F.col("level")))
)

result = (
    labeled
    .rollup("least_claim_count_level", "team")
    .agg(
        F.grouping_id().alias("gid"),
        F.sum("claim_count").alias("total_claim_count"),
    )
    .orderBy(
        F.col("least_claim_count_level").asc_nulls_last(),
        F.col("team").asc_nulls_last(),
    )
)

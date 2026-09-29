from pyspark.sql import functions as F

valid = (
    df_adjusters
    .filter((F.col("level") > 1) & (F.upper(F.trim(F.col("adjuster_name"))).like("E%")))
    .select(
        "id",
        "adjuster_name",
        "team",
        "level",
        F.concat(F.col("team"), F.lit("#"), F.col("level").cast("string")).alias("team_ref"),
    )
)

scored = (
    valid
    .filter(F.col("adjuster_name").isNotNull())
    .withColumn("adjuster_name_len", F.length(F.trim(F.col("adjuster_name"))))
)

claim_stats = (
    df_claims
    .groupBy("adjuster_id")
    .agg(
        F.count("*").alias("claim_count"),
        F.min("claim_date").alias("first_claim_date"),
    )
)

summarized_rows = (
    scored.alias("s")
    .join(claim_stats.alias("c"), F.col("c.adjuster_id") == F.col("s.id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("c.claim_count"), F.lit(0)).alias("claim_count"),
        "c.first_claim_date",
    )
)

claim_stats_2 = (
    df_claims
    .groupBy("adjuster_id")
    .agg(F.min("claimed_amount").alias("min_claim_claimed_amount"))
)

with_counts = (
    summarized_rows.alias("s")
    .join(claim_stats_2.alias("c"), F.col("c.adjuster_id") == F.col("s.id"), "left")
    .select("s.*", "c.min_claim_claimed_amount")
)

result = (
    with_counts
    .groupingSets(
        [["team", "level"], ["team"], []],
        "team", "level",
    )
    .agg(
        F.grouping_id().alias("gid"),
        F.sum(F.when(F.col("adjuster_name").isNotNull(), F.col("claim_count")).otherwise(0)).alias("claim_count_adjuster_name_is_not_null"),
        F.count("*").alias("row_count"),
    )
    .select(
        F.coalesce(F.col("team"), F.lit("ALL")).alias("team"),
        "level",
        "gid",
        "claim_count_adjuster_name_is_not_null",
        "row_count",
    )
    .orderBy(F.col("team").asc_nulls_last(), F.col("level").asc_nulls_last())
)

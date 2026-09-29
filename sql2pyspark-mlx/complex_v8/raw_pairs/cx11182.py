from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_latest = Window.partitionBy("adjuster_id").orderBy(F.col("policy_end_date_2").desc(), F.col("id").desc())

filtered = (
    df_claims
    .filter(F.col("adjuster_id").isNotNull())
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_type",
        "claimed_amount",
        "status",
        F.dayofweek(F.col("claim_date")).alias("claim_date_dayofweek"),
        F.lower(F.regexp_replace(F.trim(F.col("claim_type")), " ", "_")).alias("claim_type_slug"),
    )
)

combined_details_adjuster = (
    filtered.alias("s")
    .join(df_adjusters.alias("a"), (F.col("a.id") == F.col("s.adjuster_id")) & (F.col("a.team").isNotNull()), "inner")
    .join(df_policies.alias("p"), F.col("p.id") == F.col("s.policy_id"), "inner")
    .select(
        "s.id",
        "s.policy_id",
        "s.adjuster_id",
        "s.claim_type",
        "s.claim_type_slug",
        "a.adjuster_name",
        F.col("a.team").alias("adjuster_team"),
        F.col("a.level").alias("adjuster_level"),
        F.col("p.end_date").alias("policy_end_date"),
    )
)

enriched = (
    combined_details_adjuster
    .filter(F.upper(F.trim(F.col("adjuster_name"))).like("B%"))
    .select(
        "*",
        F.lower(F.regexp_replace(F.trim(F.col("claim_type")), " ", "_")).alias("claim_type_slug_2"),
        F.coalesce(F.col("claim_type"), F.lit("UNKNOWN")).alias("claim_type_or_unknown"),
        F.date_format(F.col("policy_end_date"), "yyyy-MM").alias("policy_end_date_label"),
    )
)

claim_type_stats = (
    enriched
    .groupBy("claim_type")
    .agg(F.max("adjuster_level").alias("max_adjuster_level"))
)

above_average = (
    enriched.alias("s")
    .join(claim_type_stats.alias("g"), F.col("g.claim_type") == F.col("s.claim_type"), "inner")
    .filter(F.col("s.adjuster_level") == F.col("g.max_adjuster_level"))
    .select(
        "s.*",
        "g.max_adjuster_level",
        F.round(F.col("s.adjuster_level") - F.col("g.max_adjuster_level"), 2).alias("adjuster_level_minus_max"),
    )
)

claim_stats = (
    df_claims
    .groupBy("policy_id")
    .agg(
        F.count(F.when(F.col("claim_type") == "collision", 1)).alias("claim_claim_type_count"),
    )
)

summarized_rows = (
    above_average.alias("s")
    .join(claim_stats.alias("c"), F.col("c.policy_id") == F.col("s.policy_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("c.claim_claim_type_count"), F.lit(0)).alias("claim_claim_type_count"),
    )
)

combined_details = (
    summarized_rows.alias("s")
    .join(df_adjusters.alias("a"), F.col("a.id") == F.col("s.adjuster_id"), "left")
    .join(df_policies.alias("p"), F.col("p.id") == F.col("s.policy_id"), "left")
    .select(
        "s.id",
        "s.policy_id",
        "s.adjuster_id",
        "s.claim_type",
        "s.claim_type_slug",
        "s.adjuster_team",
        "s.adjuster_level",
        "s.policy_end_date",
        "s.claim_type_slug_2",
        "s.policy_end_date_label",
        "s.max_adjuster_level",
        "s.adjuster_level_minus_max",
        "s.claim_claim_type_count",
        F.col("a.team").alias("adjuster_team_2"),
        F.col("a.adjuster_name").alias("adjuster_adjuster_name"),
        F.col("p.end_date").alias("policy_end_date_2"),
    )
)

unique_rows = (
    combined_details
    .withColumn("row_num", F.row_number().over(w_latest))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

result = (
    unique_rows
    .rollup("adjuster_id", "adjuster_team_2")
    .agg(
        F.percentile_approx("max_adjuster_level", 0.5).alias("median_max_adjuster_level"),
    )
    .select(
        "adjuster_id",
        F.coalesce(F.col("adjuster_team_2"), F.lit("ALL")).alias("adjuster_team_2"),
        "median_max_adjuster_level",
    )
    .orderBy(
        F.col("adjuster_id").asc_nulls_last(),
        F.col("adjuster_team_2").asc_nulls_last(),
    )
)

from pyspark.sql import functions as F

prepared = (
    df_claims
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "claim_type",
        "claimed_amount",
        "approved_amount",
        "status",
        F.greatest(F.col("approved_amount"), F.col("claimed_amount")).alias("greatest_approved_amount_claimed_amount"),
    )
)

combined_details_adjuster = (
    prepared.alias("s")
    .join(df_adjusters.alias("a"), F.col("a.id") == F.col("s.adjuster_id"), "inner")
    .select(
        "s.id",
        "s.policy_id",
        "s.adjuster_id",
        "s.claim_type",
        "s.approved_amount",
        "s.status",
        "s.greatest_approved_amount_claimed_amount",
        F.col("a.team").alias("adjuster_team"),
        F.col("a.level").alias("adjuster_level"),
        "a.adjuster_name",
    )
)

adjuster_id_stats = (
    combined_details_adjuster
    .groupBy("adjuster_id")
    .agg(F.max("adjuster_level").alias("max_adjuster_level"))
)

outliers = (
    combined_details_adjuster.alias("s")
    .join(adjuster_id_stats.alias("g"), F.col("g.adjuster_id") == F.col("s.adjuster_id"), "inner")
    .filter(F.col("s.adjuster_level") == F.col("g.max_adjuster_level"))
    .select(
        "s.*",
        "g.max_adjuster_level",
        F.round(F.col("s.adjuster_level") - F.col("g.max_adjuster_level"), 2).alias("adjuster_level_minus_max"),
    )
)

with_details = (
    outliers.alias("s")
    .join(df_adjusters.alias("a"), F.col("a.id") == F.col("s.adjuster_id"), "inner")
    .filter(F.col("a.adjuster_name").isNotNull())
    .select("s.*", F.col("a.adjuster_name").alias("adjuster_adjuster_name"))
)

result = (
    with_details
    .groupingSets(
        [["adjuster_team", "policy_id"], ["adjuster_team"], []],
        "adjuster_team", "policy_id",
    )
    .agg(
        F.grouping_id().alias("grouping_level"),
        F.round(F.stddev("greatest_approved_amount_claimed_amount"), 2).alias("stddev_greatest_approved_amount_claimed_amount"),
        F.sum(F.when(F.upper(F.trim(F.col("adjuster_name"))).like("F%"), F.col("approved_amount")).otherwise(0)).alias("approved_amount_upper_trim_adjuster_name_lik"),
    )
    .orderBy(
        F.col("adjuster_team").asc_nulls_last(),
        F.col("policy_id").asc_nulls_last(),
    )
)

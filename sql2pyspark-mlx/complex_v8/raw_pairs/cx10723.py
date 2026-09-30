from pyspark.sql import functions as F

cleaned = (
    df_claims
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "claim_type",
        "claimed_amount",
        "approved_amount",
    )
)

derived = (
    cleaned
    .filter(F.col("approved_amount") < 14993.0)
    .withColumn("claim_type_slug", F.lower(F.regexp_replace(F.trim(F.col("claim_type")), " ", "_")))
)

scored = (
    derived
    .filter(F.col("claimed_amount") >= 4698.6)
    .select(
        "id",
        "adjuster_id",
        "claim_date",
        "claim_type",
        "claimed_amount",
        "approved_amount",
        "claim_type_slug",
        F.round(F.col("approved_amount") / F.nullif(F.col("claimed_amount"), F.lit(0)), 3).alias("approved_amount_per_claimed_amount"),
        F.abs(F.col("claimed_amount") - 18364.1).alias("claimed_amount_distance"),
    )
)

joined_adjuster = (
    scored.alias("s")
    .join(df_adjusters.alias("a"), F.col("a.id") == F.col("s.adjuster_id"), "inner")
    .select(
        "s.*",
        "a.adjuster_name",
        F.col("a.level").alias("adjuster_level"),
        F.col("a.team").alias("adjuster_team"),
    )
)

result = (
    joined_adjuster
    .select(
        "id",
        "adjuster_id",
        "claim_date",
        "claim_type",
        "claim_type_slug",
        "approved_amount_per_claimed_amount",
        "claimed_amount_distance",
        "adjuster_level",
        F.when(F.col("adjuster_team").isin("Property", "Motor"), "group_a").when(F.col("adjuster_team") == "Travel", "group_b").otherwise("other").alias("adjuster_team_group"),
        F.abs(F.col("claimed_amount") - 19347.2).alias("claimed_amount_distance_2"),
    )
    .orderBy(F.col("claimed_amount_distance_2").desc())
)

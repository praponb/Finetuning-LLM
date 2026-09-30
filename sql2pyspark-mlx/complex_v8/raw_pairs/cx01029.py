from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_dedup = Window.partitionBy("status").orderBy(F.col("claimed_amount").desc(), F.col("id").desc())

valid_claims = (
    df_claims
    .filter((F.col("approved_amount") < 20843.1) & (F.col("claim_date").isNotNull()))
)

tagged = (
    valid_claims
    .filter(F.col("claim_type") == "flood")
    .select(
        "id",
        "adjuster_id",
        "claim_date",
        "claim_type",
        "claimed_amount",
        "approved_amount",
        "status",
        F.coalesce(F.col("approved_amount"), F.lit(0.0)).alias("approved_amount_filled"),
        F.when(F.col("approved_amount") >= 17759.3, "HIGH").when(F.col("approved_amount") >= 7204.1, "MEDIUM").otherwise("LOW").alias("approved_amount_tier"),
    )
)

claim_stats = (
    df_claims
    .groupBy("adjuster_id")
    .agg(
        F.max("approved_amount").alias("max_claim_approved_amount"),
        F.max("approved_amount").alias("max_claim_approved_amount_2"),
    )
)

with_counts = (
    tagged.alias("s")
    .join(claim_stats.alias("c"), F.col("c.adjuster_id") == F.col("s.adjuster_id"), "left")
    .select("s.*", "c.max_claim_approved_amount", "c.max_claim_approved_amount_2")
)

latest_per_key = (
    with_counts
    .withColumn("recency_rank", F.row_number().over(w_dedup))
    .filter(F.col("recency_rank") == 1)
    .drop("recency_rank")
)

with_parent_adjuster = (
    latest_per_key.alias("s")
    .join(df_adjusters.alias("a"), F.col("a.id") == F.col("s.adjuster_id"), "inner")
    .filter(F.col("a.team") == "Health")
    .select(
        "s.id",
        "s.adjuster_id",
        "s.claim_date",
        "s.claim_type",
        "s.approved_amount",
        "s.status",
        "s.approved_amount_filled",
        "s.approved_amount_tier",
        "s.max_claim_approved_amount_2",
        F.col("a.team").alias("adjuster_team"),
        "a.adjuster_name",
    )
)

enriched = (
    with_parent_adjuster.alias("s")
    .join(df_adjusters.alias("a"), F.col("a.id") == F.col("s.adjuster_id"), "inner")
    .select(
        "s.id",
        "s.adjuster_id",
        "s.claim_date",
        "s.approved_amount",
        "s.status",
        "s.approved_amount_filled",
        "s.adjuster_team",
        "s.adjuster_name",
        F.col("a.team").alias("adjuster_team_2"),
        F.col("a.adjuster_name").alias("adjuster_adjuster_name"),
        F.col("a.level").alias("adjuster_level"),
    )
)

with_parent = (
    enriched.alias("s")
    .join(df_adjusters.alias("a"), F.col("a.id") == F.col("s.adjuster_id"), "left")
    .select(
        "s.id",
        "s.adjuster_id",
        "s.status",
        "s.adjuster_team",
        "s.adjuster_name",
        "s.adjuster_team_2",
        "s.adjuster_level",
        F.col("a.level").alias("adjuster_level_2"),
        F.col("a.team").alias("adjuster_team_3"),
        F.col("a.adjuster_name").alias("adjuster_adjuster_name_2"),
    )
)

result = (
    with_parent
    .select(
        "id",
        "adjuster_id",
        "adjuster_name",
        "adjuster_team_2",
        "adjuster_level",
        "adjuster_team_3",
        F.when(F.col("adjuster_level_2") >= 3, "A").when(F.col("adjuster_level_2") >= 1, "B").otherwise("C").alias("adjuster_level_2_tier"),
    )
    .orderBy(F.col("id").asc_nulls_last(), F.col("adjuster_name").desc())
)

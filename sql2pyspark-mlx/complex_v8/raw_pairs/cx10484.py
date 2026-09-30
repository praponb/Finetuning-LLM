from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_team_or_unknown_level = Window.partitionBy("team_or_unknown", "level")
w_all_by_level_full = Window.orderBy("level", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

scoped = (
    df_adjusters
    .filter((F.col("level") <= 3) & (F.col("team") == "Health"))
    .select(
        "id",
        "adjuster_name",
        "team",
        "level",
        F.upper(F.trim(F.col("team"))).alias("team_clean"),
        F.coalesce(F.col("team"), F.lit("UNKNOWN")).alias("team_or_unknown"),
    )
)

claim_stats = (
    df_claims
    .groupBy("adjuster_id")
    .agg(
        F.min("claim_date").alias("first_claim_date"),
        F.min("reported_at").alias("first_claim_reported_at"),
        F.count("*").alias("claim_count"),
    )
)

with_stats = (
    scoped.alias("s")
    .join(claim_stats.alias("c"), F.col("c.adjuster_id") == F.col("s.id"), "left")
    .select(
        "s.*",
        "c.first_claim_date",
        "c.first_claim_reported_at",
        F.coalesce(F.col("c.claim_count"), F.lit(0)).alias("claim_count"),
    )
)

qualified = (
    with_stats.alias("s")
    .join(df_claims.alias("c"), (F.col("c.adjuster_id") == F.col("s.id")) & (F.col("c.claim_date").between(F.lit("2024-03-15").cast("date"), F.lit("2025-11-25").cast("date"))) & (F.col("c.status").isin("REJECTED", "IN_REVIEW", "PAID")), "left_anti")
)

windowed = (
    qualified
    .select(
        "id",
        "adjuster_name",
        "level",
        "team_or_unknown",
        "first_claim_date",
        "first_claim_reported_at",
        "claim_count",
        F.round(F.col("level") / F.nullif(F.sum("level").over(w_team_or_unknown_level), F.lit(0)), 4).alias("level_share_of_team_or_unknown_level"),
        F.first("first_claim_date").over(w_all_by_level_full).alias("first_first_claim_date"),
    )
)

result = (
    windowed
    .groupBy("first_claim_date", "first_claim_reported_at")
    .pivot("team_or_unknown", ["Motor", "Property"])
    .agg(F.min("level").alias("min_v"), F.count("level").alias("n"))
    .orderBy("first_claim_date", "first_claim_reported_at")
)

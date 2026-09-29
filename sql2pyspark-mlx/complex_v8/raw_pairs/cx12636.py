from pyspark.sql import functions as F

cleaned = (
    df_adjusters
    .filter(F.col("level") < 3)
    .select(
        "id",
        "adjuster_name",
        "team",
        "level",
    )
)

qualified = (
    cleaned.alias("s")
    .join(df_claims.alias("c"), (F.col("c.adjuster_id") == F.col("s.id")) & (F.col("c.claimed_amount") < 33975.8), "left_anti")
)

labeled = (
    qualified
    .filter(F.col("team") == "Motor")
    .select(
        "id",
        "adjuster_name",
        "level",
        F.upper(F.trim(F.col("team"))).alias("team_clean"),
        F.substring(F.trim(F.col("adjuster_name")), 1, 4).alias("adjuster_name_prefix"),
    )
)

result = (
    labeled
    .select(
        "id",
        "adjuster_name",
        "level",
        "team_clean",
        "adjuster_name_prefix",
        F.concat(F.col("adjuster_name"), F.lit("#"), F.col("id").cast("string")).alias("adjuster_name_ref"),
        F.coalesce(F.col("level"), F.lit(0)).alias("level_filled"),
    )
    .orderBy(F.col("adjuster_name_prefix").asc_nulls_last(), F.col("level").asc())
)

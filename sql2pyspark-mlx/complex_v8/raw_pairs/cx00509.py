from pyspark.sql import functions as F

valid = (
    df_listeners
    .select(
        "id",
        "username",
        "country",
        "birth_year",
        "interests",
        F.lower(F.regexp_replace(F.trim(F.col("country")), " ", "_")).alias("country_slug"),
    )
)

scored = (
    valid
    .select(
        "id",
        "username",
        "country",
        "birth_year",
        "interests",
        "country_slug",
        F.concat(F.col("username"), F.lit("#"), F.col("birth_year").cast("string")).alias("username_ref"),
    )
)

without_matches = (
    scored.alias("s")
    .join(df_plays.alias("p"), (F.col("p.listener_id") == F.col("s.id")) & (F.col("p.played_at") < F.lit("2025-02-12 11:00:00").cast("timestamp")) & (~F.col("p.is_skipped")), "left_anti")
)

enriched = (
    without_matches
    .select(
        "id",
        "username",
        "country",
        "birth_year",
        "interests",
        "username_ref",
        F.size(F.col("interests")).alias("interests_count"),
        F.lower(F.regexp_replace(F.trim(F.col("country")), " ", "_")).alias("country_slug_2"),
    )
)

detail_rows = (
    enriched.alias("s")
    .join(df_plays.alias("p"), F.col("p.listener_id") == F.col("s.id"), "left")
    .select(
        "s.id",
        "s.country",
        "s.interests",
        "s.username_ref",
        "s.interests_count",
        F.col("p.id").alias("play_id"),
        F.col("p.is_skipped").alias("play_is_skipped"),
        F.col("p.device").alias("play_device"),
    )
)

result = (
    detail_rows
    .select("play_id", "id", "interests_count")
    .distinct()
    .orderBy(F.col("id").desc_nulls_first(), F.col("play_id").asc())
)

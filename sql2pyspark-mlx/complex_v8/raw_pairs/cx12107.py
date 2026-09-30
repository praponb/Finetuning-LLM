from pyspark.sql import functions as F

recent = (
    df_plays
    .filter(~F.col("is_skipped"))
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "ms_played",
        "is_skipped",
        "source",
        F.concat(F.col("source"), F.lit("#"), F.col("track_id").cast("string")).alias("source_ref"),
        F.when(F.col("device").isin("phone", "tv"), "group_a").when(F.col("device") == "speaker", "group_b").otherwise("other").alias("device_group"),
        F.lower(F.trim(F.col("source"))).alias("source_clean"),
    )
)

screened = (
    recent.alias("s")
    .join(df_plays.alias("p"), (F.col("p.track_id") == F.col("s.track_id")) & (F.col("p.source") == "radio") & (F.col("p.device").isin("car", "speaker", "desktop")), "left_semi")
    .filter(F.col("device_group").isNotNull())
)

with_parent_listener = (
    screened.alias("s")
    .join(df_listeners.alias("l"), (F.col("l.id") == F.col("s.listener_id")) & (~F.col("l.plan").isin("premium", "student")), "left")
    .select(
        "s.id",
        "s.listener_id",
        "s.track_id",
        "s.played_at",
        "s.ms_played",
        "s.source",
        "s.source_clean",
        F.col("l.birth_year").alias("listener_birth_year"),
        F.col("l.signup_date").alias("listener_signup_date"),
        F.col("l.country").alias("listener_country"),
    )
)

result = (
    with_parent_listener
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "ms_played",
        "source_clean",
        "listener_birth_year",
        F.coalesce(F.col("listener_country"), F.lit("UNKNOWN")).alias("listener_country_or_unknown"),
    )
    .orderBy(F.col("listener_id").asc())
)

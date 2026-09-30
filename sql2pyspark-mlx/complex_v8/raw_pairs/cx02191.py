from pyspark.sql import functions as F

prepared_listeners = (
    df_listeners
    .filter((F.col("birth_year") < 1980) & (F.lower(F.col("username")).like("%is%")))
    .select(
        "id",
        "plan",
        "signup_date",
        "birth_year",
    )
)

annotated = (
    prepared_listeners
    .filter(F.col("plan") != "free")
    .select(
        "*",
        (F.floor(F.col("birth_year") / 10) * 10).alias("birth_year_bucket"),
        F.coalesce(F.col("birth_year"), F.lit(0)).alias("birth_year_filled"),
    )
)

labeled = (
    annotated
    .filter(F.col("birth_year") <= 1994)
    .select(
        "id",
        "plan",
        "birth_year",
        F.length(F.trim(F.col("plan"))).alias("plan_len"),
        F.concat(F.col("plan"), F.lit("#"), F.col("birth_year_filled").cast("string")).alias("plan_ref"),
    )
)

with_children_plays = (
    labeled.alias("s")
    .join(df_plays.alias("p"), (F.col("p.listener_id") == F.col("s.id")) & (F.col("p.ms_played") <= 251066), "inner")
    .select(
        "s.id",
        "s.birth_year",
        "s.plan_len",
        F.col("p.id").alias("play_id"),
        F.col("p.device").alias("play_device"),
        F.col("p.source").alias("play_source"),
        F.col("p.ms_played").alias("play_ms_played"),
        F.col("p.is_skipped").alias("play_is_skipped"),
    )
)

result = (
    with_children_plays
    .rollup("play_id", "play_is_skipped")
    .agg(
        F.grouping_id().alias("grouping_level"),
        F.round(F.sum("play_ms_played") / F.nullif(F.sum("birth_year"), F.lit(0)), 3).alias("play_ms_played_to_birth_year_ratio"),
        F.sum(F.when(~F.col("play_is_skipped"), F.col("birth_year")).otherwise(0)).alias("birth_year_play_is_skipped_false"),
    )
    .orderBy(
        F.col("play_id").asc_nulls_last(),
        F.col("play_is_skipped").asc_nulls_last(),
    )
)

from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_is_explicit_by_artist_debut = Window.partitionBy("is_explicit").orderBy("artist_debut_year", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_is_explicit_by_artist_debut_2 = Window.partitionBy("is_explicit").orderBy("artist_debut_year", "id")

scoped_tracks = (
    df_tracks
    .filter(F.col("release_date") >= F.lit("2024-08-26").cast("date"))
    .select(
        "id",
        "artist_id",
        "duration_sec",
        "is_explicit",
        "moods",
        (F.col("duration_sec") + F.col("bpm")).alias("duration_sec_plus_bpm"),
    )
)

enriched = (
    scoped_tracks.alias("s")
    .join(df_artists.alias("a"), F.col("a.id") == F.col("s.artist_id"), "left")
    .select(
        "s.*",
        F.col("a.country").alias("artist_country"),
        F.col("a.debut_year").alias("artist_debut_year"),
        "a.artist_name",
    )
)

screened = (
    enriched.alias("s")
    .join(df_plays.alias("p"), (F.col("p.track_id") == F.col("s.id")) & (F.col("p.is_skipped")), "left_anti")
)

metrics = (
    screened
    .select(
        "id",
        "artist_id",
        "duration_sec",
        "artist_name",
        F.first("duration_sec", ignorenulls=True).over(w_is_explicit_by_artist_debut).alias("first_known_duration_sec"),
        F.rank().over(w_is_explicit_by_artist_debut_2).alias("artist_debut_year_rank"),
    )
)

with_activity = (
    metrics.alias("s")
    .join(df_tracks.alias("t"), F.col("t.artist_id") == F.col("s.artist_id"), "left_semi")
)

with_children_plays = (
    with_activity.alias("s")
    .join(df_plays.alias("p"), F.col("p.track_id") == F.col("s.id"), "inner")
    .select(
        "s.id",
        "s.artist_id",
        "s.duration_sec",
        "s.artist_name",
        F.col("p.id").alias("play_id"),
        F.col("p.is_skipped").alias("play_is_skipped"),
        F.col("p.device").alias("play_device"),
        F.col("p.ms_played").alias("play_ms_played"),
        F.col("p.source").alias("play_source"),
    )
)

rollup_base = (
    with_children_plays
    .filter(F.col("play_device").isNotNull())
    .groupBy("artist_id", "play_source")
    .agg(
        F.count("*").alias("cnt"),
        F.sum(F.when(F.col("artist_name").isNotNull(), F.col("duration_sec"))).alias("duration_sec_artist_name_is_not_null"),
        F.percentile_approx("duration_sec", 0.5).alias("median_duration_sec"),
        F.round(100 * F.count(F.when(~F.col("play_is_skipped"), 1)) / F.count("*"), 1).alias("pct_play_is_skipped_false"),
        F.countDistinct("play_source").alias("distinct_play_sources"),
        F.array_sort(F.collect_set("play_source")).alias("play_source_list"),
    )
)

result = (
    rollup_base
    .select(
        "artist_id",
        "play_source",
        "cnt",
        "duration_sec_artist_name_is_not_null",
        "median_duration_sec",
        "pct_play_is_skipped_false",
        "distinct_play_sources",
        F.upper(F.trim(F.col("play_source"))).alias("play_source_clean"),
    )
    .orderBy("median_duration_sec")
)

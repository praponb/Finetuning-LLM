from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_duration_sec_tier_distinct_a = Window.partitionBy("duration_sec_tier", "distinct_artist_ids").orderBy("distinct_artist_ids", "duration_sec_tier").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_median_track_is_explicit_cou = Window.partitionBy("median_track_is_explicit_count")
w_median_track_is_explicit_cou_2 = Window.partitionBy("median_track_is_explicit_count", "duration_sec_tier").orderBy("distinct_artist_ids", "duration_sec_tier")
w_duration_sec_tier_distinct_a_2 = Window.partitionBy("duration_sec_tier", "distinct_artist_ids").orderBy("distinct_artist_ids", "duration_sec_tier")

base = (
    df_tracks
    .filter((F.col("release_date") < F.lit("2025-08-02").cast("date")) & (F.col("bpm") <= 156))
    .select(
        "id",
        "artist_id",
        "title",
        "duration_sec",
        "release_date",
        "is_explicit",
        "bpm",
        "moods",
        (F.col("duration_sec") - F.col("bpm")).alias("duration_sec_minus_bpm"),
        F.lower(F.regexp_replace(F.trim(F.col("title")), " ", "_")).alias("title_slug"),
        F.array_contains(F.col("moods"), "chill").alias("has_chill"),
    )
)

track_stats = (
    df_tracks
    .groupBy("artist_id")
    .agg(
        F.count(F.when(~F.col("is_explicit"), 1)).alias("track_is_explicit_count"),
        F.count("*").alias("track_count"),
        F.min("bpm").alias("min_track_bpm"),
    )
)

profiled = (
    base.alias("s")
    .join(track_stats.alias("t"), F.col("t.artist_id") == F.col("s.artist_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("t.track_is_explicit_count"), F.lit(0)).alias("track_is_explicit_count"),
        F.coalesce(F.col("t.track_count"), F.lit(0)).alias("track_count"),
        "t.min_track_bpm",
    )
)

with_parent_artist = (
    profiled.alias("s")
    .join(df_artists.alias("a"), F.col("a.id") == F.col("s.artist_id"), "left")
    .select(
        "s.id",
        "s.artist_id",
        "s.duration_sec",
        "s.release_date",
        "s.is_explicit",
        "s.bpm",
        "s.moods",
        "s.duration_sec_minus_bpm",
        "s.title_slug",
        "s.track_is_explicit_count",
        "a.artist_name",
    )
)

rollup_base = (
    with_parent_artist
    .filter(F.upper(F.trim(F.col("artist_name"))).like("S%"))
    .groupBy(
        F.when(F.col("duration_sec") >= 352, "A").when(F.col("duration_sec") >= 209, "B").otherwise("C").alias("duration_sec_tier"),
    )
    .agg(
        F.round(F.sum("duration_sec") / F.nullif(F.sum("bpm"), F.lit(0)), 3).alias("duration_sec_to_bpm_ratio"),
        F.array_sort(F.collect_set("artist_name")).alias("artist_name_list"),
        F.percentile_approx("track_is_explicit_count", 0.5).alias("median_track_is_explicit_count"),
        F.countDistinct("artist_id").alias("distinct_artist_ids"),
    )
)

sequenced = (
    rollup_base
    .select(
        "duration_sec_tier",
        "duration_sec_to_bpm_ratio",
        "artist_name_list",
        "distinct_artist_ids",
        F.last("distinct_artist_ids").over(w_duration_sec_tier_distinct_a).alias("last_distinct_artist_ids"),
        F.count(F.lit(1)).over(w_median_track_is_explicit_cou).alias("rows_in_median_track_is_explicit_count"),
        F.rank().over(w_median_track_is_explicit_cou_2).alias("distinct_artist_ids_rank"),
        F.row_number().over(w_duration_sec_tier_distinct_a_2).alias("distinct_artist_ids_seq"),
    )
)

result = (
    sequenced
    .select(
        "duration_sec_tier",
        "artist_name_list",
        "distinct_artist_ids",
        "last_distinct_artist_ids",
        "rows_in_median_track_is_explicit_count",
        "distinct_artist_ids_rank",
        "distinct_artist_ids_seq",
    )
    .orderBy("duration_sec_tier")
    .limit(5)
)

from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_is_explicit = Window.partitionBy("is_explicit")
w_artist_id_is_explicit_by_dur = Window.partitionBy("artist_id", "is_explicit").orderBy(F.col("duration_sec").desc(), "id")
w_artist_id_is_explicit_by_dur_2 = Window.partitionBy("artist_id", "is_explicit").orderBy("duration_sec", "id")
w_artist_id_is_explicit_by_dur_3 = Window.partitionBy("artist_id", "is_explicit").orderBy("duration_sec", "id").rowsBetween(-6, Window.currentRow)
w_artist_id_is_explicit_by_dur_4 = Window.partitionBy("artist_id", "is_explicit").orderBy("duration_sec", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

recent_tracks = (
    df_tracks
    .filter((F.col("release_date").between(F.lit("2024-04-29").cast("date"), F.lit("2025-08-28").cast("date"))) & (F.col("bpm").between(75, 142)))
    .select(
        "id",
        "artist_id",
        "title",
        "duration_sec",
        "is_explicit",
        "bpm",
        "moods",
        F.lower(F.regexp_replace(F.trim(F.col("title")), " ", "_")).alias("title_slug"),
        F.when(F.col("is_explicit"), 1).otherwise(0).alias("is_explicit_flag"),
    )
)

ranked = (
    recent_tracks
    .select(
        "id",
        "title",
        "duration_sec",
        "is_explicit",
        "bpm",
        "moods",
        "title_slug",
        F.round(F.col("bpm") / F.nullif(F.sum("bpm").over(w_is_explicit), F.lit(0)), 4).alias("bpm_share_of_is_explicit"),
        F.size(F.collect_set("artist_id").over(w_is_explicit)).alias("distinct_artist_id_in_is_explicit"),
        F.lag("duration_sec").over(w_artist_id_is_explicit_by_dur).alias("prev_duration_sec"),
        F.ntile(5).over(w_artist_id_is_explicit_by_dur_2).alias("duration_sec_quintile"),
        F.round(F.avg("is_explicit_flag").over(w_artist_id_is_explicit_by_dur_3), 2).alias("is_explicit_flag_avg_7"),
        F.nth_value("duration_sec", 2).over(w_artist_id_is_explicit_by_dur_4).alias("second_duration_sec"),
    )
)

without_matches = (
    ranked.alias("s")
    .join(df_plays.alias("p"), F.col("p.track_id") == F.col("s.id"), "left_semi")
)

result = (
    without_matches
    .select(
        "id",
        "title",
        "duration_sec",
        "is_explicit",
        "bpm",
        "moods",
        "title_slug",
        "bpm_share_of_is_explicit",
        "distinct_artist_id_in_is_explicit",
        "prev_duration_sec",
        "second_duration_sec",
        F.lower(F.regexp_replace(F.trim(F.col("title")), " ", "_")).alias("title_slug_2"),
    )
    .orderBy(F.col("duration_sec").desc())
)

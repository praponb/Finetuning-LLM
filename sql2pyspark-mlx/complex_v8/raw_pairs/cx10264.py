from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_is_explicit_by_bpm = Window.partitionBy("is_explicit").orderBy(F.col("bpm").desc(), "id")
w_is_explicit_by_bpm_ties = Window.partitionBy("is_explicit").orderBy(F.col("bpm").desc())
w_artist_id = Window.partitionBy("artist_id")
w_is_explicit_by_bpm_running = Window.partitionBy("is_explicit").orderBy(F.col("bpm").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

valid = (
    df_tracks
    .filter((F.col("release_date") < F.lit("2025-08-02").cast("date")) & (F.col("artist_id") <= 13))
    .select(
        "id",
        "artist_id",
        "title",
        "duration_sec",
        "is_explicit",
        "bpm",
        "moods",
        F.round(F.col("bpm") / F.nullif(F.col("duration_sec"), F.lit(0)), 3).alias("bpm_per_duration_sec"),
        F.round(F.col("duration_sec") / F.nullif(F.col("bpm"), F.lit(0)), 3).alias("duration_sec_per_bpm"),
        F.substring(F.trim(F.col("title")), 1, 2).alias("title_prefix"),
    )
)

sequenced = (
    valid
    .select(
        "id",
        "title",
        "duration_sec",
        "is_explicit",
        "bpm",
        "moods",
        "duration_sec_per_bpm",
        "title_prefix",
        F.lag("bpm").over(w_is_explicit_by_bpm).alias("prev_bpm"),
        F.round(F.cume_dist().over(w_is_explicit_by_bpm_ties), 3).alias("bpm_cume_dist"),
        F.max("bpm_per_duration_sec").over(w_artist_id).alias("artist_id_max_bpm_per_duration_sec"),
        F.last("duration_sec", ignorenulls=True).over(w_is_explicit_by_bpm_running).alias("last_known_duration_sec"),
    )
)

without_matches = (
    sequenced.alias("s")
    .join(df_plays.alias("p"), (F.col("p.track_id") == F.col("s.id")) & (F.col("p.device").isin("speaker", "car")), "left_semi")
)

filtered = (
    without_matches.alias("s")
    .join(df_plays.alias("p"), F.col("p.track_id") == F.col("s.id"), "left_semi")
)

result = (
    filtered
    .select(
        "id",
        "prev_bpm",
        "bpm",
        "last_known_duration_sec",
    )
    .unpivot(
        ids=["id"],
        values=["prev_bpm", "bpm", "last_known_duration_sec"],
        variableColumnName="field",
        valueColumnName="value",
    )
)

from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_artist_genre = Window.partitionBy("artist_genre")
w_all_by_bpm_moving_1 = Window.orderBy("bpm", "id").rowsBetween(-1, Window.currentRow)
w_all_by_duration_sec = Window.orderBy("duration_sec", "id")
w_all_by_duration_sec_ties = Window.orderBy("duration_sec")
w_artist_country_artist_genre = Window.partitionBy("artist_country", "artist_genre").orderBy("bpm", "id")
w_artist_genre_has_focus = Window.partitionBy("artist_genre", "has_focus")
w_all_by_bpm = Window.orderBy(F.col("bpm").desc(), "id")

recent = (
    df_tracks
    .filter(F.col("duration_sec") <= 357)
    .select(
        "id",
        "artist_id",
        "title",
        "duration_sec",
        "release_date",
        "bpm",
        "moods",
        (F.col("duration_sec") + F.col("bpm")).alias("duration_sec_plus_bpm"),
        F.array_contains(F.col("moods"), "focus").alias("has_focus"),
    )
)

with_parent = (
    recent.alias("s")
    .join(df_artists.alias("a"), F.col("a.id") == F.col("s.artist_id"), "left")
    .select(
        "s.id",
        "s.artist_id",
        "s.title",
        "s.duration_sec",
        "s.bpm",
        "s.moods",
        "s.has_focus",
        F.col("a.country").alias("artist_country"),
        F.col("a.genre").alias("artist_genre"),
    )
)

with_windows = (
    with_parent
    .select(
        "id",
        "artist_id",
        "title",
        "moods",
        "has_focus",
        "artist_country",
        "artist_genre",
        F.round(F.avg("duration_sec").over(w_artist_genre), 2).alias("artist_genre_avg_duration_sec"),
        F.max("duration_sec").over(w_all_by_bpm_moving_1).alias("duration_sec_max_2"),
        F.ntile(5).over(w_all_by_duration_sec).alias("duration_sec_quintile"),
        F.round(F.cume_dist().over(w_all_by_duration_sec_ties), 3).alias("duration_sec_cume_dist"),
        F.round(F.cume_dist().over(w_artist_country_artist_genre), 3).alias("bpm_cume_dist"),
        F.max("bpm").over(w_artist_genre_has_focus).alias("artist_genre_has_focus_max_bpm"),
        F.ntile(4).over(w_all_by_bpm).alias("bpm_quartile"),
        F.count(F.lit(1)).over(w_artist_genre_has_focus).alias("rows_in_artist_genre_has_focus"),
    )
)

result = (
    with_windows
    .select(
        "id",
        "title",
        "moods",
        "has_focus",
        "artist_country",
        "artist_genre",
        "artist_genre_avg_duration_sec",
        "duration_sec_max_2",
        "duration_sec_quintile",
        "duration_sec_cume_dist",
        "bpm_cume_dist",
        "bpm_quartile",
        "rows_in_artist_genre_has_focus",
    )
    .orderBy(F.col("title").desc())
)

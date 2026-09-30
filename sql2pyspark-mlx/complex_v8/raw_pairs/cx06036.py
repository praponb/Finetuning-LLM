from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_artist_genre_artist_id_by_du = Window.partitionBy("artist_genre", "artist_id").orderBy(F.col("duration_sec").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_is_explicit_by_duration_sec = Window.partitionBy("is_explicit").orderBy(F.col("duration_sec").desc(), "id").rowsBetween(-1, -1)
w_all_by_bpm = Window.orderBy(F.col("bpm").desc(), "id")
w_is_explicit = Window.partitionBy("is_explicit")
w_artist_genre_by_duration_sec = Window.partitionBy("artist_genre").orderBy("duration_sec")
w_artist_country = Window.partitionBy("artist_country")
w_artist_genre_artist_id_by_du_2 = Window.partitionBy("artist_genre", "artist_id").orderBy(F.col("duration_sec").desc())

scoped_tracks = (
    df_tracks
    .filter(~F.col("is_explicit"))
    .select(
        "id",
        "artist_id",
        "title",
        "duration_sec",
        "release_date",
        "is_explicit",
        "bpm",
        "moods",
        F.round(F.col("duration_sec") / F.nullif(F.col("bpm"), F.lit(0)), 3).alias("duration_sec_per_bpm"),
    )
)

with_parent_artist = (
    scoped_tracks.alias("s")
    .join(df_artists.alias("a"), F.col("a.id") == F.col("s.artist_id"), "left")
    .select(
        "s.id",
        "s.artist_id",
        "s.duration_sec",
        "s.is_explicit",
        "s.bpm",
        "s.duration_sec_per_bpm",
        F.col("a.genre").alias("artist_genre"),
        F.col("a.country").alias("artist_country"),
        "a.artist_name",
    )
)

with_windows = (
    with_parent_artist
    .select(
        "id",
        "artist_id",
        "duration_sec",
        "is_explicit",
        "bpm",
        "artist_genre",
        "artist_country",
        "artist_name",
        F.last("duration_sec_per_bpm", ignorenulls=True).over(w_artist_genre_artist_id_by_du).alias("last_known_duration_sec_per_bpm"),
        F.max("bpm").over(w_is_explicit_by_duration_sec).alias("bpm_max_1_prior"),
        F.lag("duration_sec_per_bpm", 1, 0).over(w_all_by_bpm).alias("prev_duration_sec_per_bpm"),
        F.sum("duration_sec_per_bpm").over(w_is_explicit).alias("is_explicit_sum_duration_sec_per_bpm"),
        F.max("duration_sec").over(w_is_explicit).alias("is_explicit_max_duration_sec"),
        F.round(F.percent_rank().over(w_artist_genre_by_duration_sec), 3).alias("duration_sec_percent_rank"),
        F.count(F.lit(1)).over(w_artist_country).alias("rows_in_artist_country"),
        F.round(F.col("duration_sec") - F.avg("duration_sec").over(w_is_explicit), 2).alias("duration_sec_vs_is_explicit_avg"),
        F.round(F.col("duration_sec_per_bpm") - F.avg("duration_sec_per_bpm").over(w_is_explicit), 2).alias("duration_sec_per_bpm_vs_is_explicit_avg"),
        F.round(F.cume_dist().over(w_artist_genre_artist_id_by_du_2), 3).alias("duration_sec_cume_dist"),
    )
)

annotated = (
    with_windows
    .filter(F.col("duration_sec_cume_dist") <= 0.6)
    .select(
        "id",
        "is_explicit",
        "artist_name",
        "prev_duration_sec_per_bpm",
        "is_explicit_sum_duration_sec_per_bpm",
        "is_explicit_max_duration_sec",
        "duration_sec_percent_rank",
        "duration_sec_per_bpm_vs_is_explicit_avg",
        "duration_sec_cume_dist",
        F.substring(F.trim(F.col("artist_name")), 1, 3).alias("artist_name_prefix"),
        F.substring(F.trim(F.col("artist_name")), 1, 2).alias("artist_name_prefix_2"),
    )
)

stats = (
    annotated
    .groupBy("is_explicit")
    .agg(
        F.array_sort(F.collect_set("artist_name")).alias("artist_name_list"),
        F.count(F.when(~F.col("is_explicit"), 1)).alias("n_is_explicit_false"),
        F.avg("prev_duration_sec_per_bpm").alias("avg_prev_duration_sec_per_bpm_check"),
    )
    .filter(F.col("avg_prev_duration_sec_per_bpm_check") > 1.0)
    .drop("avg_prev_duration_sec_per_bpm_check")
)

result = (
    stats
    .filter(F.size(F.col("artist_name_list")) >= 1)
    .select(
        "is_explicit",
        "n_is_explicit_false",
        F.when(F.col("is_explicit"), "yes").otherwise("no").alias("is_explicit_label"),
    )
    .orderBy(F.col("n_is_explicit_false").desc())
)

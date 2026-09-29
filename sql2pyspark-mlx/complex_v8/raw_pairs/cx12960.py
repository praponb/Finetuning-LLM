from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_min_duration_sec = Window.orderBy(F.col("min_duration_sec").desc(), "artist_genre", "artist_id", "is_explicit")
w_all_by_min_duration_sec_movi = Window.orderBy(F.col("min_duration_sec").desc(), "artist_genre", "artist_id", "is_explicit").rowsBetween(-1, Window.currentRow)
w_n_is_explicit_true_artist_id = Window.partitionBy("n_is_explicit_true", "artist_id")

prepared = (
    df_tracks
    .filter((F.col("release_date") >= F.lit("2024-11-14").cast("date")) & (~F.col("is_explicit")))
)

combined_details_artist = (
    prepared.alias("s")
    .join(df_artists.alias("a"), F.col("a.id") == F.col("s.artist_id"), "inner")
    .select(
        "s.id",
        "s.artist_id",
        "s.title",
        "s.duration_sec",
        "s.is_explicit",
        "s.moods",
        F.col("a.genre").alias("artist_genre"),
    )
)

totals = (
    combined_details_artist
    .groupBy("artist_genre", "artist_id", "is_explicit")
    .agg(
        F.max("duration_sec").alias("max_duration_sec"),
        F.min("duration_sec").alias("min_duration_sec"),
        F.round(F.avg("duration_sec"), 2).alias("avg_duration_sec"),
        F.count(F.when(F.col("is_explicit"), 1)).alias("n_is_explicit_true"),
    )
)

with_windows = (
    totals
    .select(
        "*",
        F.ntile(3).over(w_all_by_min_duration_sec).alias("min_duration_sec_tercile"),
        F.max("avg_duration_sec").over(w_all_by_min_duration_sec_movi).alias("avg_duration_sec_max_2"),
        F.max("avg_duration_sec").over(w_n_is_explicit_true_artist_id).alias("n_is_explicit_true_artist_id_max_avg_duration_se"),
        F.count(F.lit(1)).over(w_n_is_explicit_true_artist_id).alias("rows_in_n_is_explicit_true_artist_id"),
    )
)

result = (
    with_windows
    .filter(~F.col("is_explicit"))
    .select(
        "artist_genre",
        "artist_id",
        "max_duration_sec",
        "min_duration_sec",
        "avg_duration_sec",
        "n_is_explicit_true",
        "min_duration_sec_tercile",
        "avg_duration_sec_max_2",
        "n_is_explicit_true_artist_id_max_avg_duration_se",
        "rows_in_n_is_explicit_true_artist_id",
    )
    .orderBy("artist_genre", F.col("max_duration_sec").desc())
)

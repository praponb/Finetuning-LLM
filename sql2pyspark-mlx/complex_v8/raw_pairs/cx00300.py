from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_artist_id_by_artist_debut_ye = Window.partitionBy("artist_id").orderBy("artist_debut_year")
w_artist_id_by_artist_debut_ye_2 = Window.partitionBy("artist_id").orderBy("artist_debut_year", "id")
w_artist_id_by_artist_debut_ye_3 = Window.partitionBy("artist_id").orderBy("artist_debut_year", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

prepared = (
    df_tracks
    .filter((F.size(F.col("moods")) >= 2) & (F.col("is_explicit")))
    .select(
        "id",
        "artist_id",
        "title",
        "duration_sec",
        "release_date",
        "bpm",
        "moods",
        F.length(F.trim(F.col("title"))).alias("title_len"),
        (F.floor(F.col("duration_sec") / 50) * 50).alias("duration_sec_bucket"),
    )
)

with_details = (
    prepared.alias("s")
    .join(df_artists.alias("a"), F.col("a.id") == F.col("s.artist_id"), "left")
    .select(
        "s.id",
        "s.artist_id",
        "s.title",
        "s.duration_sec",
        "s.release_date",
        "s.bpm",
        "s.moods",
        F.col("a.debut_year").alias("artist_debut_year"),
    )
)

with_windows = (
    with_details
    .select(
        "id",
        "artist_id",
        "title",
        "release_date",
        "bpm",
        "moods",
        "artist_debut_year",
        F.round(F.cume_dist().over(w_artist_id_by_artist_debut_ye), 3).alias("artist_debut_year_cume_dist"),
        F.lag("release_date", 3).over(w_artist_id_by_artist_debut_ye_2).alias("prev3_release_date"),
        F.last("duration_sec", ignorenulls=True).over(w_artist_id_by_artist_debut_ye_3).alias("last_known_duration_sec"),
    )
)

result = (
    with_windows
    .select(
        "id",
        "artist_id",
        "title",
        "release_date",
        "bpm",
        "moods",
        "artist_debut_year",
        "prev3_release_date",
        "last_known_duration_sec",
        F.dayofweek(F.col("release_date")).alias("release_date_dayofweek"),
    )
    .orderBy("bpm")
)

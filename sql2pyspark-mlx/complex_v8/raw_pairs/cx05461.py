from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_is_explicit_artist_id_by_dur = Window.partitionBy("is_explicit", "artist_id").orderBy(F.col("duration_sec_filled").desc(), "id")
w_is_explicit_artist_id = Window.partitionBy("is_explicit", "artist_id")
w_is_explicit_artist_id_by_dur_2 = Window.partitionBy("is_explicit", "artist_id").orderBy(F.col("duration_sec_filled").desc())
w_artist_genre_is_explicit_by = Window.partitionBy("artist_genre", "is_explicit").orderBy(F.col("next2_duration_sec").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_artist_genre_is_explicit_by_2 = Window.partitionBy("artist_genre", "is_explicit").orderBy(F.col("next2_duration_sec").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

scoped_tracks = (
    df_tracks
    .filter(F.col("artist_id").isNotNull())
    .select(
        "id",
        "artist_id",
        "title",
        "duration_sec",
        "is_explicit",
        "moods",
        F.coalesce(F.col("duration_sec"), F.lit(0)).alias("duration_sec_filled"),
    )
)

with_windows = (
    scoped_tracks
    .select(
        "id",
        "artist_id",
        "title",
        "is_explicit",
        "moods",
        "duration_sec_filled",
        F.lag("duration_sec_filled", 3).over(w_is_explicit_artist_id_by_dur).alias("prev3_duration_sec_filled"),
        F.lead("duration_sec", 2).over(w_is_explicit_artist_id_by_dur).alias("next2_duration_sec"),
        F.count(F.lit(1)).over(w_is_explicit_artist_id).alias("rows_in_is_explicit_artist_id"),
        F.dense_rank().over(w_is_explicit_artist_id_by_dur_2).alias("duration_sec_filled_dense_rank"),
    )
)

derived = (
    with_windows
    .select(
        "*",
        F.lower(F.regexp_replace(F.trim(F.col("title")), " ", "_")).alias("title_slug"),
        F.coalesce(F.col("next2_duration_sec"), F.lit(0)).alias("next2_duration_sec_filled"),
    )
)

with_parent_artist = (
    derived.alias("s")
    .join(df_artists.alias("a"), F.col("a.id") == F.col("s.artist_id"), "inner")
    .select(
        "s.id",
        "s.artist_id",
        "s.title",
        "s.is_explicit",
        "s.moods",
        "s.prev3_duration_sec_filled",
        "s.next2_duration_sec",
        "s.rows_in_is_explicit_artist_id",
        "s.title_slug",
        F.col("a.genre").alias("artist_genre"),
    )
)

metrics = (
    with_parent_artist
    .select(
        "id",
        "artist_id",
        "moods",
        "next2_duration_sec",
        "rows_in_is_explicit_artist_id",
        "artist_genre",
        F.last("prev3_duration_sec_filled").over(w_artist_genre_is_explicit_by).alias("last_prev3_duration_sec_filled"),
        F.last("prev3_duration_sec_filled", ignorenulls=True).over(w_artist_genre_is_explicit_by_2).alias("last_known_prev3_duration_sec_filled"),
        F.round(F.avg("prev3_duration_sec_filled").over(w_is_explicit_artist_id), 2).alias("is_explicit_artist_id_avg_prev3_duration_sec_fil"),
        F.round(F.col("prev3_duration_sec_filled") - F.avg("prev3_duration_sec_filled").over(w_is_explicit_artist_id), 2).alias("prev3_duration_sec_filled_vs_is_explicit_artist_"),
    )
)

derived_2 = (
    metrics
    .filter(F.col("rows_in_is_explicit_artist_id").between(4, 15))
    .withColumn("next2_duration_sec_distance", F.abs(F.col("next2_duration_sec") - 352))
)

result = (
    derived_2
    .select(
        "id",
        "artist_id",
        "moods",
        "next2_duration_sec",
        "rows_in_is_explicit_artist_id",
        "is_explicit_artist_id_avg_prev3_duration_sec_fil",
        "next2_duration_sec_distance",
    )
    .orderBy(F.col("artist_id").asc(), "id")
    .limit(20)
)

from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_artist_id_by_release_date_ru = Window.partitionBy("artist_id").orderBy(F.col("release_date").desc_nulls_first(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_artist_id = Window.partitionBy("artist_id")
w_all_by_release_date_prior_3 = Window.orderBy("release_date", "id").rowsBetween(-3, -1)

valid_tracks = (
    df_tracks
    .filter((F.upper(F.trim(F.col("title"))).like("N%")) & (F.col("duration_sec") < 402))
    .select(
        "id",
        "artist_id",
        "title",
        "release_date",
        "bpm",
        "moods",
        F.substring(F.trim(F.col("title")), 1, 2).alias("title_prefix"),
    )
)

joined_artist = (
    valid_tracks.alias("s")
    .join(df_artists.alias("a"), F.col("a.id") == F.col("s.artist_id"), "inner")
    .filter(F.col("a.debut_year").between(1996, 2018))
    .select(
        "s.id",
        "s.artist_id",
        "s.title",
        "s.release_date",
        "s.bpm",
        "s.moods",
        "s.title_prefix",
        F.col("a.debut_year").alias("artist_debut_year"),
    )
)

metrics = (
    joined_artist
    .select(
        "*",
        F.count(F.lit(1)).over(w_artist_id_by_release_date_ru).alias("running_count"),
        F.sum("bpm").over(w_artist_id).alias("artist_id_sum_bpm"),
        F.sum("bpm").over(w_all_by_release_date_prior_3).alias("bpm_sum_3_prior"),
        F.count(F.lit(1)).over(w_artist_id).alias("rows_in_artist_id"),
        F.min("bpm").over(w_artist_id).alias("artist_id_min_bpm"),
        F.max("bpm").over(w_artist_id).alias("artist_id_max_bpm"),
    )
)

flattened = (
    metrics
    .select(
        "id",
        "artist_id",
        "title",
        "release_date",
        "bpm",
        "title_prefix",
        "artist_debut_year",
        "running_count",
        "artist_id_sum_bpm",
        "bpm_sum_3_prior",
        "rows_in_artist_id",
        "artist_id_min_bpm",
        "artist_id_max_bpm",
        F.explode("moods").alias("mood"),
    )
)

result = (
    flattened
    .groupBy("release_date")
    .pivot("mood", ["focus", "party", "workout", "sad"])
    .agg(F.sum("artist_id_sum_bpm"))
)

from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_artist_id = Window.partitionBy("artist_id")
w_is_explicit_artist_id_by_dur = Window.partitionBy("is_explicit", "artist_id").orderBy("duration_sec_filled")
w_artist_id_is_explicit_by_dur = Window.partitionBy("artist_id", "is_explicit").orderBy("duration_sec_filled", "id")
w_artist_id_is_explicit_by_dur_2 = Window.partitionBy("artist_id", "is_explicit").orderBy("duration_sec_filled", "id").rowsBetween(-2, Window.currentRow)

prepared = (
    df_tracks
    .filter((F.lower(F.col("title")).like("%ne%")) & (F.col("bpm").between(73, 147)))
    .select(
        "id",
        "artist_id",
        "title",
        "is_explicit",
        "bpm",
        "moods",
        F.date_format(F.col("release_date"), "yyyy-MM-dd").alias("release_date_label"),
        F.initcap(F.trim(F.col("title"))).alias("title_clean"),
        F.coalesce(F.col("duration_sec"), F.lit(0)).alias("duration_sec_filled"),
    )
)

enriched_artist = (
    prepared.alias("s")
    .join(df_artists.alias("a"), F.col("a.id") == F.col("s.artist_id"), "inner")
    .select(
        "s.id",
        "s.artist_id",
        "s.title",
        "s.is_explicit",
        "s.moods",
        "s.release_date_label",
        "s.duration_sec_filled",
        F.col("a.country").alias("artist_country"),
    )
)

metrics = (
    enriched_artist
    .select(
        "*",
        F.round(F.col("duration_sec_filled") / F.nullif(F.sum("duration_sec_filled").over(w_artist_id), F.lit(0)), 4).alias("duration_sec_filled_share_of_artist_id"),
        F.dense_rank().over(w_is_explicit_artist_id_by_dur).alias("duration_sec_filled_dense_rank"),
        (F.col("duration_sec_filled") - F.lag("duration_sec_filled").over(w_artist_id_is_explicit_by_dur)).alias("duration_sec_filled_change"),
        F.round(F.avg("duration_sec_filled").over(w_artist_id_is_explicit_by_dur_2), 2).alias("duration_sec_filled_avg_3"),
    )
)

result = (
    metrics
    .select(
        "id",
        "title",
        "duration_sec_filled",
        "artist_country",
        "duration_sec_filled_share_of_artist_id",
        "duration_sec_filled_dense_rank",
        "duration_sec_filled_change",
        "duration_sec_filled_avg_3",
    )
    .orderBy(
        F.col("duration_sec_filled_change").desc(),
        F.col("duration_sec_filled_share_of_artist_id").desc_nulls_first(),
    )
)

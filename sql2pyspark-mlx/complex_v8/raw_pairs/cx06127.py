from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_artist_debut_year = Window.orderBy(F.col("artist_debut_year").desc(), "id")
w_device_track_is_explicit = Window.partitionBy("device", "track_is_explicit")
w_track_id_by_artist_debut_yea = Window.partitionBy("track_id").orderBy(F.col("artist_debut_year").desc())

base = (
    df_plays
    .filter(F.col("track_id").isNotNull())
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "device",
        "source",
    )
)

with_details_track = (
    base.alias("s")
    .join(df_tracks.alias("t"), F.col("t.id") == F.col("s.track_id"), "inner")
    .join(df_artists.alias("a"), F.col("a.id") == F.col("t.artist_id"), "left")
    .select(
        "s.id",
        "s.listener_id",
        "s.track_id",
        "s.device",
        "s.source",
        F.col("t.title").alias("track_title"),
        F.col("t.is_explicit").alias("track_is_explicit"),
        F.col("a.debut_year").alias("artist_debut_year"),
    )
)

sequenced = (
    with_details_track
    .select(
        "id",
        "listener_id",
        "track_id",
        "device",
        "source",
        "track_title",
        "track_is_explicit",
        "artist_debut_year",
        F.lag("artist_debut_year", 3).over(w_all_by_artist_debut_year).alias("prev3_artist_debut_year"),
        F.min("artist_debut_year").over(w_device_track_is_explicit).alias("device_track_is_explicit_min_artist_debut_year"),
        F.round(F.cume_dist().over(w_track_id_by_artist_debut_yea), 3).alias("artist_debut_year_cume_dist"),
        F.round(F.col("artist_debut_year") / F.nullif(F.sum("artist_debut_year").over(w_device_track_is_explicit), F.lit(0)), 4).alias("artist_debut_year_share_of_device_track_is_expli"),
        F.dense_rank().over(w_track_id_by_artist_debut_yea).alias("artist_debut_year_dense_rank"),
    )
)

result = (
    sequenced
    .rollup("track_is_explicit", "artist_debut_year_dense_rank", "device")
    .agg(
        F.grouping_id().alias("gid"),
        F.round(F.stddev("prev3_artist_debut_year"), 2).alias("stddev_prev3_artist_debut_year"),
    )
    .orderBy(
        F.col("track_is_explicit").asc_nulls_last(),
        F.col("artist_debut_year_dense_rank").asc_nulls_last(),
        F.col("device").asc_nulls_last(),
    )
)

from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_artist_id_artist_country_by = Window.partitionBy("artist_id", "artist_country").orderBy("track_count", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_track_count_artist_id_by_rel = Window.partitionBy("track_count", "artist_id").orderBy(F.col("release_date").desc(), "id").rowsBetween(-1, -1)
w_artist_id_artist_country_by_2 = Window.partitionBy("artist_id", "artist_country").orderBy("track_count")
w_is_explicit_release_date_mon = Window.partitionBy("is_explicit", F.date_trunc("month", "release_date"))
w_track_count_artist_id_by_rel_2 = Window.partitionBy("track_count", "artist_id").orderBy(F.col("release_date").desc(), "id")

cleaned_tracks = (
    df_tracks
    .filter((F.length(F.trim(F.col("title"))) >= 5) & (F.col("is_explicit")))
    .select(
        "id",
        "artist_id",
        "release_date",
        "is_explicit",
        "moods",
    )
)

track_stats = (
    df_tracks
    .groupBy("artist_id")
    .agg(F.count("*").alias("track_count"))
)

summarized_rows = (
    cleaned_tracks.alias("s")
    .join(track_stats.alias("t"), F.col("t.artist_id") == F.col("s.artist_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("t.track_count"), F.lit(0)).alias("track_count"),
    )
)

joined_artist = (
    summarized_rows.alias("s")
    .join(df_artists.alias("a"), F.col("a.id") == F.col("s.artist_id"), "left")
    .select("s.*", F.col("a.country").alias("artist_country"))
)

ranked = (
    joined_artist
    .select(
        "id",
        "artist_id",
        "release_date",
        "is_explicit",
        "track_count",
        F.first("release_date").over(w_artist_id_artist_country_by).alias("first_release_date"),
        F.sum("track_count").over(w_track_count_artist_id_by_rel).alias("track_count_sum_1_prior"),
        F.rank().over(w_artist_id_artist_country_by_2).alias("track_count_rank"),
        F.round(F.col("track_count") / F.nullif(F.sum("track_count").over(w_is_explicit_release_date_mon), F.lit(0)), 4).alias("track_count_share_of_is_explicit_release_date_mo"),
        F.round(F.percent_rank().over(w_track_count_artist_id_by_rel_2), 3).alias("release_date_percent_rank"),
    )
)

result = (
    ranked
    .filter(F.col("is_explicit"))
    .select(
        "id",
        "artist_id",
        "release_date",
        "is_explicit",
        "track_count",
        "first_release_date",
        "track_count_sum_1_prior",
        "release_date_percent_rank",
        F.month(F.col("release_date")).alias("release_date_month"),
    )
    .orderBy(F.col("track_count_sum_1_prior").desc_nulls_first())
)

from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_genre_or_unknown_country_by = Window.partitionBy("genre_or_unknown", "country").orderBy("track_count", "id")
w_last_track_release_date_year = Window.partitionBy(F.year("last_track_release_date")).orderBy("track_count", "id")
w_is_verified = Window.partitionBy("is_verified")
w_last_track_release_date_year_2 = Window.partitionBy(F.year("last_track_release_date")).orderBy("track_count")

prepared_artists = (
    df_artists
    .filter(F.col("is_verified"))
    .select(
        "id",
        "artist_name",
        "genre",
        "country",
        "is_verified",
        F.coalesce(F.col("genre"), F.lit("UNKNOWN")).alias("genre_or_unknown"),
        F.upper(F.trim(F.col("country"))).alias("country_clean"),
        F.concat(F.col("genre"), F.lit("#"), F.col("debut_year").cast("string")).alias("genre_ref"),
    )
)

track_stats = (
    df_tracks
    .groupBy("artist_id")
    .agg(
        F.max("release_date").alias("last_track_release_date"),
        F.count("*").alias("track_count"),
    )
)

with_stats = (
    prepared_artists.alias("s")
    .join(track_stats.alias("t"), F.col("t.artist_id") == F.col("s.id"), "left")
    .select(
        "s.*",
        "t.last_track_release_date",
        F.coalesce(F.col("t.track_count"), F.lit(0)).alias("track_count"),
    )
)

with_windows = (
    with_stats
    .select(
        "id",
        "artist_name",
        "genre",
        "country",
        "is_verified",
        "genre_or_unknown",
        "country_clean",
        "genre_ref",
        F.ntile(5).over(w_genre_or_unknown_country_by).alias("track_count_quintile"),
        F.datediff(F.col("last_track_release_date"), F.lag("last_track_release_date").over(w_last_track_release_date_year)).alias("days_since_prev_last_track_release_date"),
        F.size(F.collect_set("country").over(w_is_verified)).alias("distinct_country_in_is_verified"),
        F.round(F.cume_dist().over(w_last_track_release_date_year_2), 3).alias("track_count_cume_dist"),
        F.size(F.collect_set("genre").over(w_is_verified)).alias("distinct_genre_in_is_verified"),
    )
)

genre_stats = (
    with_windows
    .groupBy("genre")
    .agg(F.avg("days_since_prev_last_track_release_date").alias("avg_days_since_prev_last_track_release_date"))
)

outliers = (
    with_windows.alias("s")
    .join(genre_stats.alias("g"), F.col("g.genre") == F.col("s.genre"), "inner")
    .filter(F.col("s.days_since_prev_last_track_release_date") > F.col("g.avg_days_since_prev_last_track_release_date"))
    .select(
        "s.*",
        "g.avg_days_since_prev_last_track_release_date",
        F.round(F.col("s.days_since_prev_last_track_release_date") - F.col("g.avg_days_since_prev_last_track_release_date"), 2).alias("days_since_prev_last_track_release_date_minus_av"),
    )
)

result = (
    outliers
    .groupBy("track_count_quintile")
    .pivot("genre_or_unknown", ["Indie", "Classical"])
    .agg(F.sum("distinct_genre_in_is_verified"))
    .orderBy("track_count_quintile")
)

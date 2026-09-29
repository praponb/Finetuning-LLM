from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_artist_id_release_date_year = Window.partitionBy("artist_id", F.year("release_date")).orderBy(F.col("release_date_year").desc())

cleaned_tracks = (
    df_tracks
    .filter((F.upper(F.trim(F.col("title"))).like("P%")) & (F.col("artist_id") <= 13))
    .select(
        "id",
        "artist_id",
        "title",
        "duration_sec",
        "release_date",
        F.date_trunc("year", F.col("release_date")).alias("release_date_year"),
    )
)

sequenced = (
    cleaned_tracks
    .withColumn("release_date_year_percent_rank", F.round(F.percent_rank().over(w_artist_id_release_date_year), 3))
    .withColumn("release_date_year_dense_rank", F.dense_rank().over(w_artist_id_release_date_year))
)

joined = (
    sequenced.alias("s")
    .join(df_artists.alias("a"), F.col("a.id") == F.col("s.artist_id"), "inner")
    .filter(F.col("a.artist_name").isNotNull())
    .select(
        "s.id",
        "s.artist_id",
        "s.title",
        "s.duration_sec",
        "s.release_date",
        "s.release_date_year_percent_rank",
        "s.release_date_year_dense_rank",
        F.col("a.is_verified").alias("artist_is_verified"),
        "a.artist_name",
    )
)

artist_id_stats = (
    joined
    .groupBy("artist_id")
    .agg(F.avg("duration_sec").alias("avg_duration_sec"))
)

above_average = (
    joined.alias("s")
    .join(artist_id_stats.alias("g"), F.col("g.artist_id") == F.col("s.artist_id"), "inner")
    .filter(F.col("s.duration_sec") > F.col("g.avg_duration_sec"))
    .select(
        "s.*",
        "g.avg_duration_sec",
        F.round(F.col("s.duration_sec") - F.col("g.avg_duration_sec"), 2).alias("duration_sec_minus_avg"),
    )
)

result = (
    above_average
    .groupingSets(
        [["artist_id", "release_date_year_dense_rank", "artist_is_verified"], ["artist_id"], ["release_date_year_dense_rank"]],
        "artist_id", "release_date_year_dense_rank", "artist_is_verified",
    )
    .agg(
        F.sum("duration_sec").alias("total_duration_sec"),
        F.min("release_date").alias("first_release_date"),
    )
    .orderBy(
        F.col("artist_id").asc_nulls_last(),
        F.col("release_date_year_dense_rank").asc_nulls_last(),
        F.col("artist_is_verified").asc_nulls_last(),
    )
)

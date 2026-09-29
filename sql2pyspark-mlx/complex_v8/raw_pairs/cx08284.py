from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_country_or_unknown_by_debut = Window.partitionBy("country_or_unknown").orderBy("debut_year", "id").rowsBetween(-2, -1)
w_country_or_unknown_by_debut_2 = Window.partitionBy("country_or_unknown").orderBy("debut_year", "id").rowsBetween(-1, Window.currentRow)
w_genre = Window.partitionBy("genre")
w_country_or_unknown_by_debut_3 = Window.partitionBy("country_or_unknown").orderBy("debut_year", "id")

recent_artists = (
    df_artists
    .filter((F.length(F.trim(F.col("artist_name"))) >= 5) & (~F.col("is_verified")))
    .select(
        "id",
        "artist_name",
        "genre",
        "country",
        "debut_year",
        "is_verified",
        F.coalesce(F.col("country"), F.lit("UNKNOWN")).alias("country_or_unknown"),
    )
)

metrics = (
    recent_artists
    .select(
        "*",
        F.sum("debut_year").over(w_country_or_unknown_by_debut).alias("debut_year_sum_2_prior"),
        F.max("debut_year").over(w_country_or_unknown_by_debut_2).alias("debut_year_max_2"),
        F.sum("debut_year").over(w_genre).alias("genre_sum_debut_year"),
        F.rank().over(w_country_or_unknown_by_debut_3).alias("debut_year_rank"),
    )
)

track_stats = (
    df_tracks
    .groupBy("artist_id")
    .agg(F.count("*").alias("track_count"), F.count("*").alias("track_count_2"))
)

with_stats = (
    metrics.alias("s")
    .join(track_stats.alias("t"), F.col("t.artist_id") == F.col("s.id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("t.track_count"), F.lit(0)).alias("track_count"),
        F.coalesce(F.col("t.track_count_2"), F.lit(0)).alias("track_count_2"),
    )
)

top_per_group = (
    with_stats
    .filter(F.col("debut_year_rank") == 1)
    .drop("debut_year_rank")
)

track_stats_2 = (
    df_tracks
    .groupBy("artist_id")
    .agg(F.min("release_date").alias("first_track_release_date"))
)

profiled = (
    top_per_group.alias("s")
    .join(track_stats_2.alias("t"), F.col("t.artist_id") == F.col("s.id"), "left")
    .select("s.*", "t.first_track_release_date")
)

derived = (
    profiled
    .select(
        "id",
        "artist_name",
        "genre",
        "country",
        "debut_year",
        "is_verified",
        "debut_year_max_2",
        "genre_sum_debut_year",
        "track_count",
        "track_count_2",
        "first_track_release_date",
        F.date_format(F.col("first_track_release_date"), "yyyy-MM-dd").alias("first_track_release_date_label"),
    )
)

result = (
    derived
    .select(
        "id",
        F.col("track_count").alias("m_track_count"),
        F.col("genre_sum_debut_year").alias("m_genre_sum_debut_year"),
        F.col("track_count_2").alias("track_count_2_v"),
    )
    .unpivot(
        ids=["id"],
        values=["m_track_count", "m_genre_sum_debut_year", "track_count_2_v"],
        variableColumnName="metric",
        valueColumnName="value",
    )
    .filter(F.col("value").isNotNull())
)

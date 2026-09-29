from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_artist_debut_year = Window.orderBy(F.col("artist_debut_year").desc_nulls_first(), "id")
w_artist_id_by_artist_debut_ye = Window.partitionBy("artist_id").orderBy(F.col("artist_debut_year").desc_nulls_first())
w_all_by_artist_debut_year_tie = Window.orderBy(F.col("artist_debut_year").desc_nulls_first())
w_artist_id_artist_country = Window.partitionBy("artist_id", "artist_country")

cleaned = (
    df_tracks
    .filter(F.upper(F.trim(F.col("title"))).like("G%"))
    .select(
        "id",
        "artist_id",
        "title",
        "release_date",
        "is_explicit",
        "bpm",
        "moods",
    )
)

without_matches = (
    cleaned
    .crossJoin(cleaned.agg(F.avg("bpm").alias("overall_avg_bpm")))
    .filter(F.col("bpm") > F.col("overall_avg_bpm"))
    .drop("overall_avg_bpm")
)

joined_artist = (
    without_matches.alias("s")
    .join(df_artists.alias("a"), (F.col("a.id") == F.col("s.artist_id")) & (F.col("a.debut_year") >= 1991), "inner")
    .select(
        "s.id",
        "s.artist_id",
        "s.title",
        "s.release_date",
        "s.is_explicit",
        "s.moods",
        F.col("a.debut_year").alias("artist_debut_year"),
        F.col("a.country").alias("artist_country"),
        F.col("a.genre").alias("artist_genre"),
    )
)

windowed = (
    joined_artist
    .select(
        "*",
        F.dense_rank().over(w_all_by_artist_debut_year).alias("artist_debut_year_dense_rank"),
        F.round(F.percent_rank().over(w_artist_id_by_artist_debut_ye), 3).alias("artist_debut_year_percent_rank"),
        F.dense_rank().over(w_all_by_artist_debut_year_tie).alias("artist_debut_year_dense_rank_2"),
        F.size(F.collect_set("artist_genre").over(w_artist_id_artist_country)).alias("distinct_artist_genre_in_artist_id_artist_countr"),
    )
)

result = (
    windowed
    .select(
        "id",
        "distinct_artist_genre_in_artist_id_artist_countr",
        "artist_debut_year_dense_rank",
    )
    .unpivot(
        ids=["id"],
        values=["distinct_artist_genre_in_artist_id_artist_countr", "artist_debut_year_dense_rank"],
        variableColumnName="measure",
        valueColumnName="metric_value",
    )
    .filter(F.col("metric_value").isNotNull())
    .orderBy("id", "measure")
)

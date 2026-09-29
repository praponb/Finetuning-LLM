from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_genre_country = Window.partitionBy("genre", "country")
w_country_by_debut_year_runnin = Window.partitionBy("country").orderBy(F.col("debut_year").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

cleaned = (
    df_artists
    .filter(F.col("is_verified"))
    .select(
        "id",
        "genre",
        "country",
        "debut_year",
        F.abs(F.col("debut_year") - 1997).alias("debut_year_distance"),
    )
)

with_trends = (
    cleaned
    .select(
        "*",
        F.max("debut_year_distance").over(w_genre_country).alias("genre_country_max_debut_year_distance"),
        F.round(F.avg("debut_year_distance").over(w_genre_country), 2).alias("genre_country_avg_debut_year_distance"),
        F.max("debut_year").over(w_genre_country).alias("genre_country_max_debut_year"),
        F.sum("debut_year_distance").over(w_country_by_debut_year_runnin).alias("running_debut_year_distance"),
    )
)

genre_stats = (
    with_trends
    .groupBy("genre")
    .agg(F.avg("debut_year").alias("avg_debut_year"))
)

vs_group = (
    with_trends.alias("s")
    .join(genre_stats.alias("g"), F.col("g.genre") == F.col("s.genre"), "inner")
    .filter(F.col("s.debut_year") > F.col("g.avg_debut_year"))
    .select(
        "s.*",
        "g.avg_debut_year",
        F.round(F.col("s.debut_year") - F.col("g.avg_debut_year"), 2).alias("debut_year_minus_avg"),
    )
)

line_level_tracks = (
    vs_group.alias("s")
    .join(df_tracks.alias("t"), F.col("t.artist_id") == F.col("s.id"), "inner")
    .select(
        "s.id",
        "s.genre",
        "s.country",
        "s.debut_year_distance",
        "s.genre_country_avg_debut_year_distance",
        "s.genre_country_max_debut_year",
        "s.running_debut_year_distance",
        "s.debut_year_minus_avg",
        F.col("t.id").alias("track_id"),
        F.col("t.moods").alias("track_moods"),
        F.col("t.title").alias("track_title"),
    )
)

result = (
    line_level_tracks
    .select(
        "id",
        "genre",
        "country",
        "genre_country_max_debut_year",
        "running_debut_year_distance",
        "debut_year_minus_avg",
        "track_id",
        "track_moods",
        F.lower(F.regexp_replace(F.trim(F.col("genre")), " ", "_")).alias("genre_slug"),
        F.concat(F.col("country"), F.lit("#"), F.col("id").cast("string")).alias("country_ref"),
    )
)

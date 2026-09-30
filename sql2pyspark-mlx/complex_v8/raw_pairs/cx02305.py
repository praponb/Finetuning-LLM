from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_is_verified_country_by_debut = Window.partitionBy("is_verified", "country").orderBy(F.col("debut_year_distance_minus_max").desc(), "id")
w_country_is_verified_by_max_d = Window.partitionBy("country", "is_verified").orderBy("max_debut_year_distance", "id")
w_is_verified_by_debut_year = Window.partitionBy("is_verified").orderBy(F.col("debut_year").desc(), "id")
w_genre = Window.partitionBy("genre")
w_is_verified_by_debut_year_mo = Window.partitionBy("is_verified").orderBy(F.col("debut_year").desc(), "id").rowsBetween(-1, Window.currentRow)
w_debut_year_dense_rank_genre = Window.partitionBy("debut_year_dense_rank", "genre")

recent = (
    df_artists
    .filter((~F.col("is_verified")) & (F.upper(F.trim(F.col("artist_name"))).like("L%")))
    .select(
        "id",
        "artist_name",
        "genre",
        "country",
        "debut_year",
        "is_verified",
        F.concat(F.col("artist_name"), F.lit("#"), F.col("debut_year").cast("string")).alias("artist_name_ref"),
        F.abs(F.col("debut_year") - 2006).alias("debut_year_distance"),
    )
)

qualified = (
    recent.alias("s")
    .join(df_tracks.alias("t"), F.col("t.artist_id") == F.col("s.id"), "left_semi")
)

genre_stats = (
    qualified
    .groupBy("genre")
    .agg(F.max("debut_year_distance").alias("max_debut_year_distance"))
)

vs_group = (
    qualified.alias("s")
    .join(genre_stats.alias("g"), F.col("g.genre") == F.col("s.genre"), "inner")
    .filter(F.col("s.debut_year_distance") == F.col("g.max_debut_year_distance"))
    .select(
        "s.*",
        "g.max_debut_year_distance",
        F.round(F.col("s.debut_year_distance") - F.col("g.max_debut_year_distance"), 2).alias("debut_year_distance_minus_max"),
    )
)

windowed = (
    vs_group
    .withColumn("rn", F.row_number().over(w_is_verified_country_by_debut))
    .withColumn("max_debut_year_distance_tercile", F.ntile(3).over(w_country_is_verified_by_max_d))
)

metrics = (
    windowed
    .select(
        "*",
        F.lag("debut_year", 3).over(w_is_verified_by_debut_year).alias("prev3_debut_year"),
        F.round(F.col("max_debut_year_distance") / F.nullif(F.sum("max_debut_year_distance").over(w_genre), F.lit(0)), 4).alias("max_debut_year_distance_share_of_genre"),
        F.max("debut_year").over(w_is_verified_by_debut_year_mo).alias("debut_year_max_2"),
        F.dense_rank().over(w_is_verified_by_debut_year).alias("debut_year_dense_rank"),
    )
)

with_trends = (
    metrics
    .select(
        "*",
        F.count(F.lit(1)).over(w_debut_year_dense_rank_genre).alias("rows_in_debut_year_dense_rank_genre"),
        F.round(F.col("debut_year_max_2") - F.avg("debut_year_max_2").over(w_debut_year_dense_rank_genre), 2).alias("debut_year_max_2_vs_debut_year_dense_rank_genre_"),
    )
)

result = (
    with_trends
    .select(
        "id",
        "artist_name",
        "genre",
        "country",
        "debut_year",
        "is_verified",
        "artist_name_ref",
        "debut_year_distance",
        "max_debut_year_distance",
        "debut_year_distance_minus_max",
        "rn",
        "max_debut_year_distance_tercile",
        "prev3_debut_year",
        "max_debut_year_distance_share_of_genre",
        "debut_year_max_2",
        "debut_year_dense_rank",
        "rows_in_debut_year_dense_rank_genre",
        "debut_year_max_2_vs_debut_year_dense_rank_genre_",
        F.least(F.col("prev3_debut_year"), F.col("debut_year_max_2")).alias("least_prev3_debut_year_debut_year_max_2"),
    )
    .orderBy(F.col("debut_year_distance_minus_max").desc())
)

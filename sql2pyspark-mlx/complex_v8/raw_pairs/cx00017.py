from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_dedup = Window.partitionBy("genre_or_unknown", "genre").orderBy(F.col("debut_year_bucket").desc(), F.col("id").desc())
w_all_by_debut_year_bucket = Window.orderBy(F.col("debut_year_bucket").desc(), "id")
w_genre_by_debut_year_bucket_f = Window.partitionBy("genre").orderBy("debut_year_bucket", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_genre = Window.partitionBy("genre")
w_all_by_debut_year_bucket_tie = Window.orderBy(F.col("debut_year_bucket").desc())

scoped_artists = (
    df_artists
    .filter((F.col("country") == "BR") & (F.col("debut_year") <= 2018))
    .select(
        "id",
        "artist_name",
        "genre",
        "debut_year",
        "is_verified",
        (F.floor(F.col("debut_year") / 5) * 5).alias("debut_year_bucket"),
        F.coalesce(F.col("genre"), F.lit("UNKNOWN")).alias("genre_or_unknown"),
    )
)

with_activity = (
    scoped_artists.alias("s")
    .join(df_tracks.alias("t"), (F.col("t.artist_id") == F.col("s.id")) & (F.col("t.release_date") < F.lit("2025-04-02").cast("date")) & (F.col("t.bpm") > 89), "left_anti")
)

latest_per_key = (
    with_activity
    .withColumn("rn", F.row_number().over(w_dedup))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

with_trends = (
    latest_per_key
    .select(
        "*",
        (F.col("debut_year") - F.lag("debut_year").over(w_all_by_debut_year_bucket)).alias("debut_year_change"),
        F.first("debut_year").over(w_genre_by_debut_year_bucket_f).alias("first_debut_year"),
        F.count(F.lit(1)).over(w_genre).alias("rows_in_genre"),
        F.dense_rank().over(w_all_by_debut_year_bucket_tie).alias("debut_year_bucket_dense_rank"),
        F.max("debut_year_bucket").over(w_genre).alias("genre_max_debut_year_bucket"),
    )
)

result = (
    with_trends
    .groupingSets(
        [["genre_or_unknown", "debut_year_bucket_dense_rank"], ["genre_or_unknown"], []],
        "genre_or_unknown", "debut_year_bucket_dense_rank",
    )
    .agg(
        F.grouping("genre_or_unknown").alias("is_total_genre_or_unknown"),
        F.sum("first_debut_year").alias("total_first_debut_year"),
    )
    .orderBy(
        F.col("genre_or_unknown").asc_nulls_last(),
        F.col("debut_year_bucket_dense_rank").asc_nulls_last(),
    )
)

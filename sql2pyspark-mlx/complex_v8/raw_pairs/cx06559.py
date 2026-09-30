from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_country_is_verified = Window.partitionBy("country", "is_verified")
w_is_verified_genre_by_debut_y = Window.partitionBy("is_verified", "genre").orderBy(F.col("debut_year_filled").desc(), "id").rowsBetween(-2, Window.currentRow)
w_is_verified_genre_by_debut_y_2 = Window.partitionBy("is_verified", "genre").orderBy(F.col("debut_year_filled").desc())

base_artists = (
    df_artists
    .filter(F.upper(F.trim(F.col("artist_name"))).like("S%"))
    .select(
        "id",
        "artist_name",
        "genre",
        "country",
        "debut_year",
        "is_verified",
        F.when(F.col("genre").isin("HipHop", "Jazz"), "group_a").when(F.col("genre") == "Pop", "group_b").otherwise("other").alias("genre_group"),
        (F.floor(F.col("debut_year") / 5) * 5).alias("debut_year_bucket"),
        F.coalesce(F.col("debut_year"), F.lit(0)).alias("debut_year_filled"),
    )
)

with_windows = (
    base_artists
    .select(
        "id",
        "country",
        "debut_year",
        "is_verified",
        "debut_year_filled",
        F.round(F.col("debut_year") / F.nullif(F.sum("debut_year").over(w_country_is_verified), F.lit(0)), 4).alias("debut_year_share_of_country_is_verified"),
        F.sum("debut_year_bucket").over(w_is_verified_genre_by_debut_y).alias("debut_year_bucket_sum_3"),
        F.dense_rank().over(w_is_verified_genre_by_debut_y_2).alias("debut_year_filled_dense_rank"),
    )
)

enriched = (
    with_windows
    .withColumn("least_debut_year_bucket_sum_3_debut_year", F.least(F.col("debut_year_bucket_sum_3"), F.col("debut_year")))
)

result = (
    enriched
    .select(
        "id",
        "debut_year",
        "debut_year_filled",
        "debut_year_filled_dense_rank",
        "least_debut_year_bucket_sum_3_debut_year",
    )
    .orderBy(F.col("debut_year").asc_nulls_last(), F.col("id").desc())
)

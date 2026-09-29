from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_genre = Window.partitionBy("genre")
w_is_verified_by_total_debut_y = Window.partitionBy("is_verified").orderBy("total_debut_year", "is_verified", "country", "genre").rowsBetween(-3, Window.currentRow)

valid_artists = (
    df_artists
    .filter((F.col("artist_name").isNotNull()) & (F.col("country") != "BR"))
    .select(
        "id",
        "artist_name",
        "genre",
        "country",
        "debut_year",
        "is_verified",
    )
)

enriched = (
    valid_artists
    .filter(F.col("debut_year") > 1996)
    .withColumn("artist_name_clean", F.initcap(F.trim(F.col("artist_name"))))
)

summary = (
    enriched
    .groupBy("is_verified", "country", "genre")
    .agg(F.sum("debut_year").alias("total_debut_year"))
)

metrics = (
    summary
    .select(
        "is_verified",
        "country",
        "genre",
        F.sum("total_debut_year").over(w_genre).alias("genre_sum_total_debut_year"),
        F.count(F.lit(1)).over(w_genre).alias("rows_in_genre"),
        F.max("total_debut_year").over(w_is_verified_by_total_debut_y).alias("total_debut_year_max_4"),
        F.round(F.col("total_debut_year") - F.avg("total_debut_year").over(w_genre), 2).alias("total_debut_year_vs_genre_avg"),
    )
)

result = (
    metrics
    .filter(F.col("rows_in_genre") <= 13)
    .select(
        "is_verified",
        "country",
        "genre",
        "genre_sum_total_debut_year",
        "rows_in_genre",
        "total_debut_year_max_4",
        "total_debut_year_vs_genre_avg",
        (F.col("rows_in_genre") - F.col("total_debut_year_max_4")).alias("rows_in_genre_minus_total_debut_year_max_4"),
    )
    .orderBy("country")
)

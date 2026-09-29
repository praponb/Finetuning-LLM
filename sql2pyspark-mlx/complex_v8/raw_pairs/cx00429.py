from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_latest = Window.partitionBy("country").orderBy(F.col("debut_year").desc(), F.col("id").desc())
w_country_by_debut_year = Window.partitionBy("country").orderBy("debut_year", "id")
w_country = Window.partitionBy("country")
w_country_by_debut_year_runnin = Window.partitionBy("country").orderBy("debut_year", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_dedup = Window.partitionBy("genre").orderBy(F.col("distinct_genre_in_country").desc(), F.col("id").desc())

scoped_artists = (
    df_artists
    .filter((F.col("debut_year") <= 2020) & (F.lower(F.col("artist_name")).like("%te%")))
    .select(
        "id",
        "artist_name",
        "genre",
        "country",
        "debut_year",
        "is_verified",
        F.abs(F.col("debut_year") - 2006).alias("debut_year_distance"),
        F.lower(F.trim(F.col("artist_name"))).alias("artist_name_clean"),
    )
)

latest = (
    scoped_artists
    .withColumn("row_num", F.row_number().over(w_latest))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

with_windows = (
    latest
    .select(
        "id",
        "genre",
        "artist_name_clean",
        F.rank().over(w_country_by_debut_year).alias("debut_year_rank"),
        F.row_number().over(w_country_by_debut_year).alias("rn"),
        F.size(F.collect_set("genre").over(w_country)).alias("distinct_genre_in_country"),
        F.min("debut_year_distance").over(w_country_by_debut_year_runnin).alias("min_debut_year_distance_so_far"),
        F.count(F.lit(1)).over(w_country).alias("rows_in_country"),
        (F.col("debut_year_distance") - F.lag("debut_year_distance").over(w_country_by_debut_year)).alias("debut_year_distance_change"),
    )
)

tagged = (
    with_windows
    .select(
        "*",
        F.length(F.trim(F.col("artist_name_clean"))).alias("artist_name_clean_len"),
        F.concat_ws(" / ", F.col("artist_name_clean"), F.col("genre")).alias("artist_name_clean_genre_label"),
    )
)

latest_per_key = (
    tagged
    .withColumn("row_num", F.row_number().over(w_dedup))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

derived = (
    latest_per_key
    .select(
        "id",
        "artist_name_clean",
        "debut_year_rank",
        "rn",
        "rows_in_country",
        "debut_year_distance_change",
        "artist_name_clean_genre_label",
        (F.floor(F.col("rn") / 2) * 2).alias("rn_bucket"),
    )
)

result = (
    derived
    .filter(F.col("artist_name_clean").isNotNull())
    .select(
        "id",
        "rows_in_country",
        "debut_year_distance_change",
        "artist_name_clean_genre_label",
        "rn_bucket",
        F.greatest(F.col("rows_in_country"), F.col("rn_bucket")).alias("greatest_rows_in_country_rn_bucket"),
    )
)

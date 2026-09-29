from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_dedup = Window.partitionBy("genre").orderBy(F.col("debut_year_distance").desc(), F.col("id").desc())

cleaned = (
    df_artists
    .filter(F.col("genre") == "Classical")
    .select(
        "id",
        "genre",
        "country",
        "debut_year",
        "is_verified",
        F.lower(F.trim(F.col("country"))).alias("country_clean"),
        F.concat_ws("-", F.col("genre"), F.col("artist_name")).alias("genre_artist_name_label"),
        F.abs(F.col("debut_year") - 1997).alias("debut_year_distance"),
    )
)

latest_per_key = (
    cleaned
    .withColumn("rn", F.row_number().over(w_dedup))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

result = (
    latest_per_key
    .groupingSets(
        [["is_verified", "genre", "country"], ["is_verified"], []],
        "is_verified", "genre", "country",
    )
    .agg(F.array_sort(F.collect_set("country")).alias("country_list"))
    .orderBy(
        F.col("is_verified").asc_nulls_last(),
        F.col("genre").asc_nulls_last(),
        F.col("country").asc_nulls_last(),
    )
)

from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_debut_year = Window.orderBy("debut_year", "id")
w_is_verified_flag_genre = Window.partitionBy("is_verified_flag", "genre")
w_dedup = Window.partitionBy("genre").orderBy(F.col("is_verified_flag").desc(), F.col("id").desc())

recent_artists = (
    df_artists
    .filter((~F.col("is_verified")) | (F.col("country").isin("KR", "TH", "JP")))
    .select(
        "id",
        "artist_name",
        "genre",
        "country",
        "debut_year",
        "is_verified",
        F.lower(F.trim(F.col("country"))).alias("country_clean"),
        F.concat(F.col("artist_name"), F.lit("#"), F.col("debut_year").cast("string")).alias("artist_name_ref"),
        F.when(F.col("is_verified"), 1).otherwise(0).alias("is_verified_flag"),
    )
)

with_trends = (
    recent_artists
    .select(
        "id",
        "genre",
        "debut_year",
        "is_verified",
        "country_clean",
        "artist_name_ref",
        "is_verified_flag",
        (F.col("is_verified_flag") - F.lag("is_verified_flag").over(w_all_by_debut_year)).alias("is_verified_flag_change"),
        F.count(F.lit(1)).over(w_is_verified_flag_genre).alias("rows_in_is_verified_flag_genre"),
    )
)

track_stats = (
    df_tracks
    .groupBy("artist_id")
    .agg(F.min("duration_sec").alias("min_track_duration_sec"))
)

with_stats = (
    with_trends.alias("s")
    .join(track_stats.alias("t"), F.col("t.artist_id") == F.col("s.id"), "left")
    .select("s.*", "t.min_track_duration_sec")
)

without_matches = (
    with_stats
    .crossJoin(with_stats.agg(F.avg("rows_in_is_verified_flag_genre").alias("overall_avg_rows_in_is_verified_flag_genre")))
    .filter(F.col("rows_in_is_verified_flag_genre") > F.col("overall_avg_rows_in_is_verified_flag_genre"))
    .drop("overall_avg_rows_in_is_verified_flag_genre")
)

unique_rows = (
    without_matches
    .withColumn("rn", F.row_number().over(w_dedup))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

scored = (
    unique_rows
    .select(
        "*",
        F.length(F.trim(F.col("artist_name_ref"))).alias("artist_name_ref_len"),
        F.greatest(F.col("min_track_duration_sec"), F.col("debut_year")).alias("greatest_min_track_duration_sec_debut_year"),
    )
)

result = (
    scored
    .select("is_verified")
    .distinct()
)

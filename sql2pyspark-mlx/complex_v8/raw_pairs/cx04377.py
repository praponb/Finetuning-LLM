from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_country_genre = Window.partitionBy("country", "genre")
w_all_by_debut_year_bucket_pri = Window.orderBy(F.col("debut_year_bucket").desc(), "id").rowsBetween(-6, -1)
w_country_by_debut_year_bucket = Window.partitionBy("country").orderBy("debut_year_bucket", "id")

valid = (
    df_artists
    .filter(F.col("genre").isin("Pop", "Classical"))
    .select(
        "id",
        "artist_name",
        "genre",
        "country",
        "debut_year",
        (F.floor(F.col("debut_year") / 5) * 5).alias("debut_year_bucket"),
    )
)

track_stats = (
    df_tracks
    .groupBy("artist_id")
    .agg(F.count("*").alias("track_count"))
)

summarized_rows = (
    valid.alias("s")
    .join(track_stats.alias("t"), F.col("t.artist_id") == F.col("s.id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("t.track_count"), F.lit(0)).alias("track_count"),
    )
)

with_activity = (
    summarized_rows
    .crossJoin(summarized_rows.agg(F.avg("debut_year_bucket").alias("overall_avg_debut_year_bucket")))
    .filter(F.col("debut_year_bucket") > F.col("overall_avg_debut_year_bucket"))
    .drop("overall_avg_debut_year_bucket")
)

screened = (
    with_activity.alias("s")
    .join(df_tracks.alias("t"), F.col("t.artist_id") == F.col("s.id"), "left_semi")
)

windowed = (
    screened
    .select(
        "id",
        "artist_name",
        "genre",
        "debut_year",
        "debut_year_bucket",
        "track_count",
        F.round(F.col("debut_year") / F.nullif(F.sum("debut_year").over(w_country_genre), F.lit(0)), 4).alias("debut_year_share_of_country_genre"),
        F.max("track_count").over(w_all_by_debut_year_bucket_pri).alias("track_count_max_6_prior"),
        F.count(F.lit(1)).over(w_country_genre).alias("rows_in_country_genre"),
        F.ntile(3).over(w_country_by_debut_year_bucket).alias("debut_year_bucket_tercile"),
    )
)

result = (
    windowed
    .rollup("debut_year_bucket_tercile", "genre")
    .agg(
        F.sum("track_count_max_6_prior").alias("total_track_count_max_6_prior"),
        F.array_sort(F.collect_set("artist_name")).alias("artist_name_list"),
        F.round(100 * F.count(F.when(F.length(F.trim(F.col("artist_name"))) >= 6, 1)) / F.count("*"), 1).alias("pct_length_trim_artist_name_6"),
    )
    .orderBy(
        F.col("debut_year_bucket_tercile").asc_nulls_last(),
        F.col("genre").asc_nulls_last(),
    )
)

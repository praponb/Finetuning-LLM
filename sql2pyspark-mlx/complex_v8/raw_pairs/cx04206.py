from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_release_date_month_is_explic = Window.partitionBy(F.month("release_date"), "is_explicit").orderBy(F.col("duration_sec_distance").desc(), "id")
w_release_date_month_is_explic_2 = Window.partitionBy(F.month("release_date"), "is_explicit").orderBy(F.col("duration_sec_distance").desc(), "id").rowsBetween(-6, Window.currentRow)
w_artist_id = Window.partitionBy("artist_id")

prepared_tracks = (
    df_tracks
    .filter(F.col("duration_sec") < 312)
    .select(
        "id",
        "artist_id",
        "duration_sec",
        "release_date",
        "is_explicit",
        "moods",
        F.abs(F.col("duration_sec") - 352).alias("duration_sec_distance"),
        F.substring(F.trim(F.col("title")), 1, 4).alias("title_prefix"),
    )
)

with_windows = (
    prepared_tracks
    .select(
        "id",
        "artist_id",
        "duration_sec",
        "is_explicit",
        "moods",
        "duration_sec_distance",
        "title_prefix",
        F.round(F.cume_dist().over(w_release_date_month_is_explic), 3).alias("duration_sec_distance_cume_dist"),
        F.round(F.avg("duration_sec").over(w_release_date_month_is_explic_2), 2).alias("duration_sec_avg_7"),
        F.round(F.col("duration_sec") - F.avg("duration_sec").over(w_artist_id), 2).alias("duration_sec_vs_artist_id_avg"),
        F.round(F.col("duration_sec") / F.nullif(F.sum("duration_sec").over(w_artist_id), F.lit(0)), 4).alias("duration_sec_share_of_artist_id"),
    )
)

derived = (
    with_windows
    .filter(F.col("title_prefix").isNotNull())
    .select(
        "id",
        "artist_id",
        "duration_sec",
        "is_explicit",
        "duration_sec_distance",
        "title_prefix",
        "duration_sec_distance_cume_dist",
        "duration_sec_avg_7",
        F.round(F.col("duration_sec") / F.nullif(F.col("duration_sec_distance"), F.lit(0)), 3).alias("duration_sec_per_duration_sec_distance"),
        F.upper(F.trim(F.col("title_prefix"))).alias("title_prefix_clean"),
    )
)

derived_2 = (
    derived
    .withColumn("duration_sec_distance_tier", F.when(F.col("duration_sec_distance") >= 273, "large").when(F.col("duration_sec_distance") >= 147, "medium").otherwise("small"))
)

result = (
    derived_2
    .groupingSets(
        [["artist_id", "duration_sec_distance_tier", "is_explicit"], ["artist_id"]],
        "artist_id", "duration_sec_distance_tier", "is_explicit",
    )
    .agg(F.count("*").alias("cnt"))
    .select(
        "artist_id",
        F.coalesce(F.col("duration_sec_distance_tier"), F.lit("ALL")).alias("duration_sec_distance_tier"),
        "is_explicit",
        "cnt",
    )
    .orderBy(
        F.col("artist_id").asc_nulls_last(),
        F.col("duration_sec_distance_tier").asc_nulls_last(),
        F.col("is_explicit").asc_nulls_last(),
    )
)

from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_debut_year_tier_by_debut_yea = Window.partitionBy("debut_year_tier").orderBy("debut_year")
w_debut_year_tier = Window.partitionBy("debut_year_tier")
w_debut_year_tier_by_debut_yea_2 = Window.partitionBy("debut_year_tier").orderBy("debut_year", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

base_artists = (
    df_artists
    .select(
        "id",
        "artist_name",
        "genre",
        "country",
        "debut_year",
        "is_verified",
        F.when(F.col("debut_year") >= 2018, "large").when(F.col("debut_year") >= 1994, "medium").otherwise("small").alias("debut_year_tier"),
    )
)

metrics = (
    base_artists
    .select(
        "id",
        "artist_name",
        "genre",
        "country",
        "debut_year",
        "is_verified",
        "debut_year_tier",
        F.round(F.percent_rank().over(w_debut_year_tier_by_debut_yea), 3).alias("debut_year_percent_rank"),
        F.sum("debut_year").over(w_debut_year_tier).alias("debut_year_tier_sum_debut_year"),
        F.count(F.lit(1)).over(w_debut_year_tier_by_debut_yea_2).alias("running_count"),
    )
)

track_stats = (
    df_tracks
    .groupBy("artist_id")
    .agg(
        F.count("*").alias("track_count"),
        F.count("*").alias("track_count_2"),
        F.min("bpm").alias("min_track_bpm"),
    )
)

with_stats = (
    metrics.alias("s")
    .join(track_stats.alias("t"), F.col("t.artist_id") == F.col("s.id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("t.track_count"), F.lit(0)).alias("track_count"),
        F.coalesce(F.col("t.track_count_2"), F.lit(0)).alias("track_count_2"),
        "t.min_track_bpm",
    )
)

track_stats_2 = (
    df_tracks
    .groupBy("artist_id")
    .agg(F.count("*").alias("track_count_3"))
)

profiled = (
    with_stats.alias("s")
    .join(track_stats_2.alias("t"), F.col("t.artist_id") == F.col("s.id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("t.track_count_3"), F.lit(0)).alias("track_count_3"),
    )
)

qualified = (
    profiled.alias("s")
    .join(df_tracks.alias("t"), (F.col("t.artist_id") == F.col("s.id")) & (F.col("t.bpm") <= 142) & (F.col("t.duration_sec") <= 312), "left_semi")
    .filter(F.col("debut_year").between(1996, 2020))
)

track_stats_3 = (
    df_tracks
    .groupBy("artist_id")
    .agg(F.round(F.avg("bpm"), 2).alias("avg_track_bpm"))
)

summarized_rows = (
    qualified.alias("s")
    .join(track_stats_3.alias("t"), F.col("t.artist_id") == F.col("s.id"), "left")
    .select("s.*", "t.avg_track_bpm")
)

result = (
    summarized_rows
    .filter(F.col("track_count_2").between(2, 6))
    .select(
        "id",
        "artist_name",
        "genre",
        "country",
        "debut_year",
        "is_verified",
        "debut_year_tier",
        "debut_year_tier_sum_debut_year",
        "running_count",
        "min_track_bpm",
        "track_count_3",
    )
    .orderBy(F.col("country").asc_nulls_last())
)

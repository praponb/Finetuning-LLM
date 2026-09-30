from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_artist_genre = Window.partitionBy("artist_genre")
w_all_by_duration_sec_running = Window.orderBy("duration_sec", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_artist_genre_by_duration_sec = Window.partitionBy("artist_genre").orderBy("duration_sec", "id")
w_artist_id_is_explicit = Window.partitionBy("artist_id", "is_explicit")
w_artist_genre_by_duration_sec_2 = Window.partitionBy("artist_genre").orderBy("duration_sec", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_duration_sec_running_2 = Window.orderBy(F.col("duration_sec").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_duration_sec_prior_3 = Window.orderBy("duration_sec", "id").rowsBetween(-3, -1)
w_all_by_duration_sec_prior_6 = Window.orderBy(F.col("duration_sec").desc(), "id").rowsBetween(-6, -1)

filtered_tracks = (
    df_tracks
    .filter((F.col("release_date").isNotNull()) & (F.col("duration_sec") < 353))
)

enriched = (
    filtered_tracks.alias("s")
    .join(df_artists.alias("a"), F.col("a.id") == F.col("s.artist_id"), "inner")
    .select(
        "s.id",
        "s.artist_id",
        "s.duration_sec",
        "s.is_explicit",
        "s.moods",
        F.col("a.genre").alias("artist_genre"),
    )
)

metrics = (
    enriched
    .select(
        "id",
        "duration_sec",
        "moods",
        F.round(F.col("duration_sec") - F.avg("duration_sec").over(w_artist_genre), 2).alias("duration_sec_vs_artist_genre_avg"),
        F.count(F.lit(1)).over(w_all_by_duration_sec_running).alias("running_count"),
        F.round(F.cume_dist().over(w_artist_genre_by_duration_sec), 3).alias("duration_sec_cume_dist"),
        F.round(F.col("duration_sec") / F.nullif(F.sum("duration_sec").over(w_artist_id_is_explicit), F.lit(0)), 4).alias("duration_sec_share_of_artist_id_is_explicit"),
        F.min("duration_sec").over(w_artist_genre_by_duration_sec_2).alias("min_duration_sec_so_far"),
        F.first("duration_sec", ignorenulls=True).over(w_all_by_duration_sec_running_2).alias("first_known_duration_sec"),
        F.count(F.lit(1)).over(w_artist_id_is_explicit).alias("rows_in_artist_id_is_explicit"),
        F.round(F.avg("duration_sec").over(w_all_by_duration_sec_prior_3), 2).alias("duration_sec_avg_3_prior"),
        F.min("duration_sec").over(w_artist_genre).alias("artist_genre_min_duration_sec"),
        F.round(F.avg("duration_sec").over(w_all_by_duration_sec_prior_6), 2).alias("duration_sec_avg_6_prior"),
    )
)

tagged = (
    metrics
    .select(
        "*",
        (F.col("first_known_duration_sec") - F.col("duration_sec_cume_dist")).alias("first_known_duration_sec_minus_duration_sec_cume"),
        (F.floor(F.col("running_count") / 5) * 5).alias("running_count_bucket"),
    )
)

summary = (
    tagged
    .groupBy(
        F.when(F.col("min_duration_sec_so_far") >= 3584, "HIGH").when(F.col("min_duration_sec_so_far") >= 912, "MEDIUM").otherwise("LOW").alias("min_duration_sec_so_far_tier"),
    )
    .agg(F.countDistinct("id").alias("distinct_ids"))
)

result = (
    summary
    .select(
        "min_duration_sec_so_far_tier",
        "distinct_ids",
        F.abs(F.col("distinct_ids") - 6).alias("distinct_ids_distance"),
    )
)

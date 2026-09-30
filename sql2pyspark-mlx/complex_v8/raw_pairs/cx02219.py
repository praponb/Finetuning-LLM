from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_duration_sec_ties = Window.orderBy(F.col("duration_sec").asc_nulls_last())
w_all_by_duration_sec = Window.orderBy("duration_sec", "id")
w_all_by_duration_sec_running = Window.orderBy(F.col("duration_sec").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_artist_id = Window.partitionBy("artist_id")
w_all_by_duration_sec_running_2 = Window.orderBy("duration_sec", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_duration_sec_moving_6 = Window.orderBy(F.col("duration_sec").asc_nulls_last(), "id").rowsBetween(-6, Window.currentRow)
w_all_by_duration_sec_2 = Window.orderBy(F.col("duration_sec").desc(), "id")
w_all_by_duration_sec_3 = Window.orderBy(F.col("duration_sec").asc_nulls_last(), "id")

prepared = (
    df_tracks
    .select(
        "id",
        "artist_id",
        "title",
        "duration_sec",
        "release_date",
        "bpm",
        "moods",
        F.substring(F.trim(F.col("title")), 1, 2).alias("title_prefix"),
        F.coalesce(F.col("duration_sec"), F.lit(0)).alias("duration_sec_filled"),
    )
)

with_parent = (
    prepared.alias("s")
    .join(df_artists.alias("a"), F.col("a.id") == F.col("s.artist_id"), "left")
    .select(
        "s.id",
        "s.artist_id",
        "s.title",
        "s.duration_sec",
        "s.bpm",
        "s.moods",
        "s.duration_sec_filled",
        F.col("a.country").alias("artist_country"),
        F.col("a.genre").alias("artist_genre"),
        F.col("a.debut_year").alias("artist_debut_year"),
    )
)

metrics = (
    with_parent
    .select(
        "*",
        F.round(F.percent_rank().over(w_all_by_duration_sec_ties), 3).alias("duration_sec_percent_rank"),
        F.row_number().over(w_all_by_duration_sec).alias("rn"),
        F.sum("duration_sec").over(w_all_by_duration_sec_running).alias("running_duration_sec"),
        F.last("artist_debut_year", ignorenulls=True).over(w_all_by_duration_sec_running).alias("last_known_artist_debut_year"),
        F.max("duration_sec").over(w_artist_id).alias("artist_id_max_duration_sec"),
        F.count(F.lit(1)).over(w_all_by_duration_sec_running_2).alias("running_count"),
        F.round(F.avg("duration_sec").over(w_all_by_duration_sec_moving_6), 2).alias("duration_sec_avg_7"),
        F.round(F.cume_dist().over(w_all_by_duration_sec_2), 3).alias("duration_sec_cume_dist"),
        F.round(F.cume_dist().over(w_all_by_duration_sec_3), 3).alias("duration_sec_cume_dist_2"),
    )
)

per_group = (
    metrics
    .groupBy("artist_country", "artist_id")
    .agg(
        F.sum(F.when(F.length(F.trim(F.col("title"))) >= 6, 1).otherwise(0)).alias("n_length_trim_title_6"),
        F.countDistinct("artist_genre").alias("distinct_artist_genres"),
        F.array_sort(F.collect_set("artist_country")).alias("artist_country_list"),
        F.round(F.avg("duration_sec_percent_rank"), 2).alias("avg_duration_sec_percent_rank"),
        F.percentile_approx("rn", 0.5).alias("median_rn"),
    )
)

result = (
    per_group
    .select(
        "artist_country",
        "artist_id",
        "n_length_trim_title_6",
        "distinct_artist_genres",
        "avg_duration_sec_percent_rank",
    )
    .orderBy(
        F.col("avg_duration_sec_percent_rank").desc(),
        F.col("n_length_trim_title_6").asc(),
        "artist_country",
        "artist_id",
    )
    .limit(5)
)

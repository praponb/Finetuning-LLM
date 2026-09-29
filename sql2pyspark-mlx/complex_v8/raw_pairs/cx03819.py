from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_duration_sec = Window.orderBy(F.col("duration_sec").desc(), "id")
w_artist_id = Window.partitionBy("artist_id")
w_is_explicit_artist_id_by_bpm = Window.partitionBy("is_explicit", "artist_id").orderBy(F.col("bpm").asc_nulls_last(), "id")
w_is_explicit_artist_id_by_bpm_2 = Window.partitionBy("is_explicit", "artist_id").orderBy(F.col("bpm").asc_nulls_last())
w_artist_id_by_bpm_running = Window.partitionBy("artist_id").orderBy("bpm", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_artist_id_by_max_rows_in_art = Window.partitionBy("artist_id").orderBy(F.col("max_rows_in_artist_id").desc_nulls_first())
w_artist_id_by_max_rows_in_art_2 = Window.partitionBy("artist_id").orderBy(F.col("max_rows_in_artist_id").desc_nulls_first(), "artist_id")

cleaned_tracks = (
    df_tracks
    .filter((F.col("release_date").isNotNull()) & (F.col("duration_sec") <= 352))
    .select(
        "id",
        "artist_id",
        "title",
        "duration_sec",
        "is_explicit",
        "bpm",
        "moods",
    )
)

windowed = (
    cleaned_tracks
    .select(
        "id",
        "artist_id",
        "title",
        "moods",
        (F.col("bpm") - F.lag("bpm").over(w_all_by_duration_sec)).alias("bpm_change"),
        F.min("duration_sec").over(w_artist_id).alias("artist_id_min_duration_sec"),
        F.max("duration_sec").over(w_artist_id).alias("artist_id_max_duration_sec"),
        F.rank().over(w_is_explicit_artist_id_by_bpm).alias("bpm_rank"),
        F.count(F.lit(1)).over(w_artist_id).alias("rows_in_artist_id"),
        F.round(F.col("duration_sec") - F.avg("duration_sec").over(w_artist_id), 2).alias("duration_sec_vs_artist_id_avg"),
        F.round(F.percent_rank().over(w_is_explicit_artist_id_by_bpm_2), 3).alias("bpm_percent_rank"),
        F.last("duration_sec", ignorenulls=True).over(w_artist_id_by_bpm_running).alias("last_known_duration_sec"),
    )
)

first_rows = (
    windowed
    .filter(F.col("bpm_rank") <= 5)
    .drop("bpm_rank")
)

per_group = (
    first_rows
    .groupBy("artist_id")
    .agg(
        F.sum(F.when(F.upper(F.trim(F.col("title"))).like("R%"), F.col("bpm_change")).otherwise(0)).alias("bpm_change_upper_trim_title_like_r"),
        F.array_sort(F.collect_set("title")).alias("title_list"),
        F.round(F.sum("last_known_duration_sec") / F.nullif(F.sum("rows_in_artist_id"), F.lit(0)), 3).alias("last_known_duration_sec_to_rows_in_artist_id_rat"),
        F.max("rows_in_artist_id").alias("max_rows_in_artist_id"),
        F.min("bpm_percent_rank").alias("min_bpm_percent_rank"),
    )
)

ranked = (
    per_group
    .withColumn("max_rows_in_artist_id_dense_rank", F.dense_rank().over(w_artist_id_by_max_rows_in_art))
    .withColumn("min_bpm_percent_rank_change", F.col("min_bpm_percent_rank") - F.lag("min_bpm_percent_rank").over(w_artist_id_by_max_rows_in_art_2))
)

result = (
    ranked
    .select(
        "artist_id",
        "bpm_change_upper_trim_title_like_r",
        "last_known_duration_sec_to_rows_in_artist_id_rat",
        "max_rows_in_artist_id",
        "max_rows_in_artist_id_dense_rank",
        "min_bpm_percent_rank_change",
        F.greatest(F.col("max_rows_in_artist_id"), F.col("min_bpm_percent_rank")).alias("greatest_max_rows_in_artist_id_min_bpm_percent_r"),
        F.round(F.col("min_bpm_percent_rank") / F.nullif(F.col("max_rows_in_artist_id"), F.lit(0)), 3).alias("min_bpm_percent_rank_per_max_rows_in_artist_id"),
    )
    .orderBy(
        F.col("last_known_duration_sec_to_rows_in_artist_id_rat").asc_nulls_last(),
        F.col("min_bpm_percent_rank_per_max_rows_in_artist_id").desc(),
    )
)

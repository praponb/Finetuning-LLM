from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_is_explicit_artist_id = Window.partitionBy("is_explicit", "artist_id")
w_is_explicit_artist_id_by_rel = Window.partitionBy("is_explicit", "artist_id").orderBy(F.col("release_date").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_artist_id_is_explicit_by_bpm = Window.partitionBy("artist_id", "is_explicit").orderBy(F.col("bpm").desc(), "id")
w_all_by_bpm_ties = Window.orderBy(F.col("bpm").desc())
w_all_by_bpm_running = Window.orderBy(F.col("bpm").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_bpm = Window.orderBy(F.col("bpm").desc(), "id")
w_all_by_release_date_ties = Window.orderBy(F.col("release_date").asc_nulls_last())
w_all_by_release_date_moving_2 = Window.orderBy(F.col("release_date").asc_nulls_last(), "id").rowsBetween(-2, Window.currentRow)
w_is_explicit_artist_id_by_rel_2 = Window.partitionBy("is_explicit", "artist_id").orderBy(F.col("release_date").desc())

cleaned = (
    df_tracks
    .filter(~F.col("is_explicit"))
    .select(
        "id",
        "artist_id",
        "title",
        "release_date",
        "is_explicit",
        "bpm",
        "moods",
        F.lower(F.regexp_replace(F.trim(F.col("title")), " ", "_")).alias("title_slug"),
    )
)

ranked = (
    cleaned
    .select(
        "id",
        "artist_id",
        "title",
        "is_explicit",
        "bpm",
        "moods",
        "title_slug",
        F.last("bpm", ignorenulls=True).over(w_is_explicit_artist_id_by_rel).alias("last_known_bpm"),
        F.lag("release_date", 3).over(w_artist_id_is_explicit_by_bpm).alias("prev3_release_date"),
        F.count(F.lit(1)).over(w_is_explicit_artist_id).alias("rows_in_is_explicit_artist_id"),
        F.round(F.percent_rank().over(w_all_by_bpm_ties), 3).alias("bpm_percent_rank"),
        F.count(F.lit(1)).over(w_all_by_bpm_running).alias("running_count"),
        F.lag("release_date").over(w_all_by_bpm).alias("prev_release_date"),
        F.min("bpm").over(w_is_explicit_artist_id).alias("is_explicit_artist_id_min_bpm"),
        F.round(F.percent_rank().over(w_all_by_release_date_ties), 3).alias("release_date_percent_rank"),
        F.round(F.col("bpm") / F.nullif(F.sum("bpm").over(w_is_explicit_artist_id), F.lit(0)), 4).alias("bpm_share_of_is_explicit_artist_id"),
        F.max("bpm").over(w_all_by_release_date_moving_2).alias("bpm_max_3"),
        F.round(F.cume_dist().over(w_is_explicit_artist_id_by_rel_2), 3).alias("release_date_cume_dist"),
    )
)

tagged = (
    ranked
    .select(
        "id",
        "artist_id",
        "is_explicit",
        "bpm",
        "title_slug",
        "prev3_release_date",
        "rows_in_is_explicit_artist_id",
        "bpm_percent_rank",
        "running_count",
        "is_explicit_artist_id_min_bpm",
        "release_date_percent_rank",
        "bpm_share_of_is_explicit_artist_id",
        "bpm_max_3",
        "release_date_cume_dist",
        F.date_add(F.col("prev_release_date"), 365).alias("prev_release_date_plus_365d"),
        F.date_format(F.col("prev_release_date"), "yyyy-MM").alias("prev_release_date_label"),
    )
)

result = (
    tagged
    .select(
        "id",
        "artist_id",
        "is_explicit",
        "bpm",
        "title_slug",
        "rows_in_is_explicit_artist_id",
        "bpm_percent_rank",
        "running_count",
        "is_explicit_artist_id_min_bpm",
        "bpm_share_of_is_explicit_artist_id",
        "bpm_max_3",
        "release_date_cume_dist",
        "prev_release_date_label",
        F.datediff(F.lit("2025-06-30").cast("date"), F.col("prev3_release_date")).alias("days_since_prev3_release_date"),
    )
    .orderBy(F.col("release_date_cume_dist").desc(), "is_explicit_artist_id_min_bpm")
)

from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_is_explicit_by_bpm_full = Window.partitionBy("is_explicit").orderBy(F.col("bpm").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_duration_sec_running = Window.orderBy(F.col("duration_sec").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_is_explicit_duration_sec_tie = Window.partitionBy("is_explicit", "duration_sec_tier")
w_all_by_duration_sec_full = Window.orderBy(F.col("duration_sec").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_duration_sec_ties = Window.orderBy(F.col("duration_sec").desc())
w_all_by_duration_sec = Window.orderBy(F.col("duration_sec").desc(), "id")
w_all_by_duration_sec_moving_2 = Window.orderBy(F.col("duration_sec").desc(), "id").rowsBetween(-2, Window.currentRow)

filtered_tracks = (
    df_tracks
    .filter((F.lower(F.col("title")).like("%lo%")) & (F.col("duration_sec") < 353))
    .select(
        "id",
        "artist_id",
        "title",
        "duration_sec",
        "release_date",
        "is_explicit",
        "bpm",
        "moods",
        F.when(F.col("duration_sec") >= 365, "HIGH").when(F.col("duration_sec") >= 165, "MEDIUM").otherwise("LOW").alias("duration_sec_tier"),
    )
)

metrics = (
    filtered_tracks
    .select(
        "*",
        F.last("bpm").over(w_is_explicit_by_bpm_full).alias("last_bpm"),
        F.last("release_date", ignorenulls=True).over(w_all_by_duration_sec_running).alias("last_known_release_date"),
        F.size(F.collect_set("artist_id").over(w_is_explicit_duration_sec_tie)).alias("distinct_artist_id_in_is_explicit_duration_sec_t"),
        F.count(F.lit(1)).over(w_is_explicit_duration_sec_tie).alias("rows_in_is_explicit_duration_sec_tier"),
        F.round(F.col("duration_sec") / F.nullif(F.sum("duration_sec").over(w_is_explicit_duration_sec_tie), F.lit(0)), 4).alias("duration_sec_share_of_is_explicit_duration_sec_t"),
        F.max("duration_sec").over(w_is_explicit_duration_sec_tie).alias("is_explicit_duration_sec_tier_max_duration_sec"),
        F.first("bpm").over(w_all_by_duration_sec_full).alias("first_bpm"),
        F.count(F.lit(1)).over(w_all_by_duration_sec_running).alias("running_count"),
        F.dense_rank().over(w_all_by_duration_sec_ties).alias("duration_sec_dense_rank"),
        F.min("bpm").over(w_all_by_duration_sec_running).alias("min_bpm_so_far"),
        F.row_number().over(w_all_by_duration_sec).alias("seq_no"),
        F.max("bpm").over(w_all_by_duration_sec_moving_2).alias("bpm_max_3"),
        F.ntile(4).over(w_all_by_duration_sec).alias("duration_sec_quartile"),
    )
)

top_rows = (
    metrics
    .filter(F.col("duration_sec_dense_rank") == 1)
    .drop("duration_sec_dense_rank")
)

result = (
    top_rows
    .select(
        "id",
        "artist_id",
        "title",
        "duration_sec",
        "release_date",
        "is_explicit",
        "moods",
        "duration_sec_tier",
        "last_bpm",
        "distinct_artist_id_in_is_explicit_duration_sec_t",
        "rows_in_is_explicit_duration_sec_tier",
        "duration_sec_share_of_is_explicit_duration_sec_t",
        "is_explicit_duration_sec_tier_max_duration_sec",
        "running_count",
        "min_bpm_so_far",
        "seq_no",
        "bpm_max_3",
    )
    .orderBy(F.col("id").asc(), F.col("duration_sec_tier").asc_nulls_last())
    .limit(20)
)

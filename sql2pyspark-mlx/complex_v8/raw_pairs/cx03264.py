from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_is_explicit_artist_id = Window.partitionBy("is_explicit", "artist_id")
w_all_by_duration_sec_ties = Window.orderBy("duration_sec")
w_all_by_duration_sec = Window.orderBy("duration_sec", "id")
w_all_by_duration_sec_running = Window.orderBy("duration_sec", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

valid = (
    df_tracks
    .select(
        "id",
        "artist_id",
        "title",
        "duration_sec",
        "is_explicit",
        F.round(F.col("duration_sec") / F.nullif(F.col("bpm"), F.lit(0)), 3).alias("duration_sec_per_bpm"),
        F.coalesce(F.col("bpm"), F.lit(0)).alias("bpm_filled"),
    )
)

with_activity = (
    valid.alias("s")
    .join(df_plays.alias("p"), F.col("p.track_id") == F.col("s.id"), "left_semi")
)

ranked = (
    with_activity
    .select(
        "*",
        F.round(F.col("bpm_filled") - F.avg("bpm_filled").over(w_is_explicit_artist_id), 2).alias("bpm_filled_vs_is_explicit_artist_id_avg"),
        F.round(F.cume_dist().over(w_all_by_duration_sec_ties), 3).alias("duration_sec_cume_dist"),
        F.rank().over(w_all_by_duration_sec_ties).alias("duration_sec_rank"),
        F.row_number().over(w_all_by_duration_sec).alias("seq_no"),
        F.first("duration_sec_per_bpm", ignorenulls=True).over(w_all_by_duration_sec_running).alias("first_known_duration_sec_per_bpm"),
    )
)

result = (
    ranked
    .filter(F.col("seq_no").between(1, 9))
    .select(
        "id",
        "artist_id",
        "title",
        "is_explicit",
        "duration_sec_per_bpm",
        "bpm_filled",
        "bpm_filled_vs_is_explicit_artist_id_avg",
        "duration_sec_cume_dist",
        "seq_no",
        "first_known_duration_sec_per_bpm",
        F.round(F.col("seq_no") / F.nullif(F.col("duration_sec"), F.lit(0)), 3).alias("seq_no_per_duration_sec"),
        F.greatest(F.col("duration_sec_per_bpm"), F.col("duration_sec_cume_dist")).alias("greatest_duration_sec_per_bpm_duration_sec_cume_"),
    )
    .orderBy(
        F.col("first_known_duration_sec_per_bpm").desc_nulls_first(),
        F.col("title").desc(),
        "id",
    )
    .limit(10)
)

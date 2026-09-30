from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_played_at = Window.orderBy(F.col("played_at").desc(), "id")
w_all_by_played_at_2 = Window.orderBy("played_at", "id")
w_all_by_played_at_ties = Window.orderBy(F.col("played_at").desc())
w_all_by_played_at_full = Window.orderBy("played_at", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_track_id_is_skipped_by_playe = Window.partitionBy("track_id", "is_skipped").orderBy(F.col("played_at").asc_nulls_last(), "id")

base = (
    df_plays
    .filter((F.col("track_id") >= 28) & (~F.col("device").isin("car", "speaker")))
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "device",
        "is_skipped",
        "source",
    )
)

ranked = (
    base
    .select(
        "*",
        F.row_number().over(w_all_by_played_at).alias("rn"),
        F.ntile(5).over(w_all_by_played_at_2).alias("played_at_quintile"),
        F.round((F.unix_timestamp("played_at") - F.unix_timestamp(F.lag("played_at").over(w_all_by_played_at))) / 60, 1).alias("minutes_since_prev_played_at"),
        F.rank().over(w_all_by_played_at).alias("played_at_rank"),
        F.round(F.cume_dist().over(w_all_by_played_at_ties), 3).alias("played_at_cume_dist"),
        F.nth_value("played_at", 2).over(w_all_by_played_at_full).alias("second_played_at"),
        F.ntile(5).over(w_track_id_is_skipped_by_playe).alias("played_at_quintile_2"),
        F.ntile(10).over(w_track_id_is_skipped_by_playe).alias("played_at_decile"),
        F.row_number().over(w_all_by_played_at_2).alias("rn_2"),
        F.round(F.percent_rank().over(w_track_id_is_skipped_by_playe), 3).alias("played_at_percent_rank"),
        F.rank().over(w_track_id_is_skipped_by_playe).alias("played_at_rank_2"),
        F.round(F.cume_dist().over(w_all_by_played_at), 3).alias("played_at_cume_dist_2"),
        F.ntile(3).over(w_all_by_played_at_2).alias("played_at_tercile"),
    )
)

result = (
    ranked
    .filter(F.col("second_played_at") < F.lit("2025-02-18 12:00:00").cast("timestamp"))
    .select(
        "id",
        "listener_id",
        "played_at",
        "device",
        "is_skipped",
        "source",
        "rn",
        "played_at_quintile",
        "played_at_rank",
        "played_at_cume_dist",
        "played_at_quintile_2",
        "played_at_decile",
        "played_at_percent_rank",
        "played_at_rank_2",
        "played_at_cume_dist_2",
        "played_at_tercile",
        F.when(F.col("played_at_cume_dist") >= 0.8, "HIGH").when(F.col("played_at_cume_dist") >= 0.3, "MEDIUM").otherwise("LOW").alias("played_at_cume_dist_tier"),
        F.concat_ws(" / ", F.col("device"), F.col("source")).alias("device_source_label"),
    )
    .orderBy(F.col("played_at_quintile_2").desc(), "id")
    .limit(5)
)

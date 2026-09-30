from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_source_len_track_id_by_playe = Window.partitionBy("source_len", "track_id").orderBy(F.col("played_at").asc_nulls_last(), "id")
w_source_len_track_id_by_playe_2 = Window.partitionBy("source_len", "track_id").orderBy(F.col("played_at").asc_nulls_last())
w_device = Window.partitionBy("device")
w_source_by_played_at = Window.partitionBy("source").orderBy("played_at", "id")
w_source_by_played_at_running = Window.partitionBy("source").orderBy("played_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_source_listener_id_by_source = Window.partitionBy("source", "listener_id").orderBy("source_len")
w_source_len_track_id_by_playe_3 = Window.partitionBy("source_len", "track_id").orderBy(F.col("played_at").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_source_len_listener_id_by_pl = Window.partitionBy("source_len", "listener_id").orderBy(F.col("played_at").desc_nulls_first(), "id").rowsBetween(-3, -1)
w_source_by_played_at_moving_1 = Window.partitionBy("source").orderBy("played_at", "id").rowsBetween(-1, Window.currentRow)
w_source_listener_id_by_source_2 = Window.partitionBy("source", "listener_id").orderBy("source_len", "id")

cleaned_plays = (
    df_plays
    .filter(F.month(F.col("played_at")).isin(1, 2))
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "device",
        "source",
        F.length(F.trim(F.col("source"))).alias("source_len"),
    )
)

with_trends = (
    cleaned_plays
    .select(
        "*",
        F.ntile(4).over(w_source_len_track_id_by_playe).alias("played_at_quartile"),
        F.round(F.cume_dist().over(w_source_len_track_id_by_playe), 3).alias("played_at_cume_dist"),
        F.rank().over(w_source_len_track_id_by_playe_2).alias("played_at_rank"),
        F.round(F.col("source_len") - F.avg("source_len").over(w_device), 2).alias("source_len_vs_device_avg"),
        F.max("source_len").over(w_device).alias("device_max_source_len"),
        F.ntile(10).over(w_source_by_played_at).alias("played_at_decile"),
        F.max("source_len").over(w_source_by_played_at_running).alias("max_source_len_so_far"),
        F.rank().over(w_source_listener_id_by_source).alias("source_len_rank"),
        F.ntile(3).over(w_source_len_track_id_by_playe).alias("played_at_tercile"),
        F.nth_value("played_at", 2).over(w_source_len_track_id_by_playe_3).alias("second_played_at"),
        F.size(F.collect_set("track_id").over(w_device)).alias("distinct_track_id_in_device"),
        F.round(F.avg("source_len").over(w_source_len_listener_id_by_pl), 2).alias("source_len_avg_3_prior"),
        F.round(F.avg("source_len").over(w_source_by_played_at_moving_1), 2).alias("source_len_avg_2"),
        F.round(F.cume_dist().over(w_source_listener_id_by_source_2), 3).alias("source_len_cume_dist"),
    )
)

top_per_group = (
    with_trends
    .filter(F.col("source_len_rank") <= 5)
    .drop("source_len_rank")
)

result = (
    top_per_group
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "source",
        "played_at_quartile",
        "played_at_cume_dist",
        "device_max_source_len",
        "played_at_decile",
        "max_source_len_so_far",
        "played_at_tercile",
        "second_played_at",
        "distinct_track_id_in_device",
        "source_len_avg_3_prior",
        "source_len_avg_2",
        "source_len_cume_dist",
        F.round((F.unix_timestamp(F.col("played_at")) - F.unix_timestamp(F.col("second_played_at"))) / 3600, 1).alias("hours_second_played_at_to_played_at"),
        F.concat(F.col("source"), F.lit("#"), F.col("device_max_source_len").cast("string")).alias("source_ref"),
    )
    .orderBy("played_at", "id")
    .limit(10)
)

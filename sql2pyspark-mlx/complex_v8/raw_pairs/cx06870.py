from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_device = Window.partitionBy("device")
w_listener_id_by_played_at_run = Window.partitionBy("listener_id").orderBy("played_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_listener_id_by_ms_played_run = Window.partitionBy("listener_id").orderBy(F.col("ms_played").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_listener_id_device_by_ms_pla = Window.partitionBy("listener_id", "device").orderBy(F.col("ms_played").desc(), "id")
w_listener_id_device_by_ms_pla_2 = Window.partitionBy("listener_id", "device").orderBy(F.col("ms_played").desc(), "id").rowsBetween(-1, Window.currentRow)
w_listener_id_by_ms_played_tie = Window.partitionBy("listener_id").orderBy(F.col("ms_played").desc())
w_listener_id_by_played_at_ful = Window.partitionBy("listener_id").orderBy("played_at", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

prepared_plays = (
    df_plays
    .filter((F.col("source") == "radio") & (F.col("listener_id") >= 9))
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "ms_played",
        "device",
        F.date_format(F.col("played_at"), "yyyy-MM-dd").alias("played_at_label"),
    )
)

sequenced = (
    prepared_plays
    .select(
        "id",
        "track_id",
        "played_at",
        "ms_played",
        "played_at_label",
        F.max("ms_played").over(w_device).alias("device_max_ms_played"),
        F.round(F.avg("ms_played").over(w_device), 2).alias("device_avg_ms_played"),
        F.max("ms_played").over(w_listener_id_by_played_at_run).alias("max_ms_played_so_far"),
        F.round(F.col("ms_played") / F.nullif(F.sum("ms_played").over(w_device), F.lit(0)), 4).alias("ms_played_share_of_device"),
        F.last("played_at", ignorenulls=True).over(w_listener_id_by_ms_played_run).alias("last_known_played_at"),
        F.rank().over(w_listener_id_device_by_ms_pla).alias("ms_played_rank"),
        F.sum("ms_played").over(w_listener_id_device_by_ms_pla_2).alias("ms_played_sum_2"),
        F.sum("ms_played").over(w_device).alias("device_sum_ms_played"),
        F.size(F.collect_set("listener_id").over(w_device)).alias("distinct_listener_id_in_device"),
        F.count(F.lit(1)).over(w_device).alias("rows_in_device"),
        F.round(F.cume_dist().over(w_listener_id_by_ms_played_tie), 3).alias("ms_played_cume_dist"),
        F.size(F.collect_set("track_id").over(w_device)).alias("distinct_track_id_in_device"),
        F.nth_value("played_at", 2).over(w_listener_id_by_played_at_ful).alias("second_played_at"),
    )
)

result = (
    sequenced
    .select(
        "id",
        "track_id",
        "played_at",
        "ms_played",
        "played_at_label",
        "device_max_ms_played",
        "device_avg_ms_played",
        "ms_played_share_of_device",
        "last_known_played_at",
        "ms_played_rank",
        "distinct_listener_id_in_device",
        "rows_in_device",
        "ms_played_cume_dist",
        "distinct_track_id_in_device",
        "second_played_at",
    )
    .orderBy("played_at", F.col("ms_played").asc())
)

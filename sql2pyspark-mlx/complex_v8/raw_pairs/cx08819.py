from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_play_source_play_played_at_y = Window.partitionBy("play_source", F.year("play_played_at")).orderBy(F.col("play_played_at").desc(), "id", "play_id")
w_listener_id_play_id_by_play = Window.partitionBy("listener_id", "play_id").orderBy("play_played_at", "id", "play_id")
w_play_source_play_played_at_y_2 = Window.partitionBy("play_source", F.year("play_played_at")).orderBy(F.col("play_played_at").desc(), "id", "play_id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

valid = (
    df_plays
    .filter(F.col("ms_played") > 145720)
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "device",
        "source",
    )
)

detail_rows = (
    valid.alias("s")
    .join(df_plays.alias("p"), (F.col("p.listener_id") == F.col("s.listener_id")) & (F.col("p.track_id") >= 23), "inner")
    .select(
        "s.id",
        "s.listener_id",
        "s.track_id",
        "s.device",
        F.col("p.id").alias("play_id"),
        F.col("p.source").alias("play_source"),
        F.col("p.track_id").alias("play_track_id"),
        F.col("p.played_at").alias("play_played_at"),
        F.col("p.device").alias("play_device"),
    )
)

ranked = (
    detail_rows
    .select(
        "id",
        "listener_id",
        "track_id",
        "play_id",
        "play_track_id",
        "play_device",
        F.row_number().over(w_play_source_play_played_at_y).alias("seq_no"),
        F.ntile(3).over(w_play_source_play_played_at_y).alias("play_played_at_tercile"),
        F.round((F.unix_timestamp("play_played_at") - F.unix_timestamp(F.lag("play_played_at").over(w_listener_id_play_id_by_play))) / 60, 1).alias("minutes_since_prev_play_played_at"),
        F.first("play_played_at").over(w_play_source_play_played_at_y_2).alias("first_play_played_at"),
        F.row_number().over(w_listener_id_play_id_by_play).alias("seq_no_2"),
        F.lead("play_played_at", 3).over(w_listener_id_play_id_by_play).alias("next3_play_played_at"),
    )
)

top_per_group = ranked.filter(F.col("seq_no") <= 2)

result = (
    top_per_group
    .groupBy("play_track_id")
    .pivot("play_device", ["desktop", "speaker", "tv"])
    .agg(F.min("seq_no_2"))
)

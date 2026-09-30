from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_source_is_skipped = Window.partitionBy("source", "is_skipped")
w_listener_id_track_id = Window.partitionBy("listener_id", "track_id")
w_track_id_by_ms_played = Window.partitionBy("track_id").orderBy("ms_played", "id")
w_listener_id_source_by_ms_pla = Window.partitionBy("listener_id", "source").orderBy(F.col("ms_played").desc(), "id")
w_track_id_source_by_ms_played = Window.partitionBy("track_id", "source").orderBy("ms_played", "id")
w_listener_id_source_by_ms_pla_2 = Window.partitionBy("listener_id", "source").orderBy(F.col("ms_played").desc(), "id").rowsBetween(-3, Window.currentRow)
w_is_skipped_device_by_ms_play = Window.partitionBy("is_skipped", "device").orderBy(F.col("ms_played").desc(), "id")
w_all_by_ms_played_ties = Window.orderBy("ms_played")

scoped_plays = (
    df_plays
    .filter((F.col("device") == "phone") | (~F.col("is_skipped")))
    .select(
        "id",
        "listener_id",
        "track_id",
        "ms_played",
        "device",
        "is_skipped",
        "source",
        F.substring(F.trim(F.col("source")), 1, 4).alias("source_prefix"),
    )
)

sequenced = (
    scoped_plays
    .select(
        "id",
        "listener_id",
        "track_id",
        "ms_played",
        "device",
        "is_skipped",
        "source",
        "source_prefix",
        F.round(F.col("ms_played") / F.nullif(F.sum("ms_played").over(w_source_is_skipped), F.lit(0)), 4).alias("ms_played_share_of_source_is_skipped"),
        F.round(F.avg("ms_played").over(w_listener_id_track_id), 2).alias("listener_id_track_id_avg_ms_played"),
        F.dense_rank().over(w_track_id_by_ms_played).alias("ms_played_dense_rank"),
        F.rank().over(w_listener_id_source_by_ms_pla).alias("ms_played_rank"),
        F.row_number().over(w_track_id_source_by_ms_played).alias("rn"),
        F.round(F.avg("ms_played").over(w_listener_id_source_by_ms_pla_2), 2).alias("ms_played_avg_4"),
        (F.col("ms_played") - F.lag("ms_played").over(w_listener_id_source_by_ms_pla)).alias("ms_played_change"),
        F.lead("ms_played").over(w_is_skipped_device_by_ms_play).alias("next_ms_played"),
        F.round(F.percent_rank().over(w_all_by_ms_played_ties), 3).alias("ms_played_percent_rank"),
    )
)

top_rows = (
    sequenced
    .filter(F.col("rn") == 1)
    .drop("rn")
)

result = (
    top_rows
    .select(
        "id",
        "listener_id",
        "device",
        "is_skipped",
        "ms_played_dense_rank",
        "ms_played_rank",
        "ms_played_avg_4",
        "ms_played_change",
        "next_ms_played",
        "ms_played_percent_rank",
        F.when(F.col("is_skipped"), "yes").otherwise("no").alias("is_skipped_label"),
    )
    .orderBy("ms_played_percent_rank")
)

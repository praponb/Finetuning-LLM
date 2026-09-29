from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_track_id_by_played_at_quarte = Window.partitionBy("track_id").orderBy(F.col("played_at_quarter").desc())
w_track_id_by_played_at_quarte_2 = Window.partitionBy("track_id").orderBy(F.col("played_at_quarter").desc(), "id")
w_track_id_by_played_at_quarte_3 = Window.partitionBy("track_id").orderBy(F.col("played_at_quarter").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

prepared = (
    df_plays
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "ms_played",
        "device",
        "is_skipped",
        F.quarter(F.col("played_at")).alias("played_at_quarter"),
        F.abs(F.col("ms_played") - 166517).alias("ms_played_distance"),
        (F.floor(F.col("ms_played") / 50000) * 50000).alias("ms_played_bucket"),
    )
)

metrics = (
    prepared
    .select(
        "*",
        F.dense_rank().over(w_track_id_by_played_at_quarte).alias("played_at_quarter_dense_rank"),
        F.lead("ms_played_distance").over(w_track_id_by_played_at_quarte_2).alias("next_ms_played_distance"),
        F.first("ms_played_distance", ignorenulls=True).over(w_track_id_by_played_at_quarte_3).alias("first_known_ms_played_distance"),
        F.dense_rank().over(w_track_id_by_played_at_quarte_2).alias("played_at_quarter_dense_rank_2"),
        F.round((F.unix_timestamp("played_at") - F.unix_timestamp(F.lag("played_at").over(w_track_id_by_played_at_quarte_2))) / 60, 1).alias("minutes_since_prev_played_at"),
    )
)

play_stats = (
    df_plays
    .groupBy("track_id")
    .agg(
        F.count(F.when(~F.col("is_skipped"), 1)).alias("play_is_skipped_count"),
    )
)

with_stats = (
    metrics.alias("s")
    .join(play_stats.alias("p"), F.col("p.track_id") == F.col("s.track_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("p.play_is_skipped_count"), F.lit(0)).alias("play_is_skipped_count"),
    )
)

filtered = (
    with_stats.alias("s")
    .join(df_plays.alias("p"), (F.col("p.track_id") == F.col("s.track_id")) & (~F.col("p.device").isin("speaker", "desktop")), "left_semi")
)

result = (
    filtered
    .select(
        "id",
        "listener_id",
        "track_id",
        "device",
        "played_at_quarter",
        "next_ms_played_distance",
        "played_at_quarter_dense_rank_2",
        "play_is_skipped_count",
        (F.floor(F.col("next_ms_played_distance") / 50000) * 50000).alias("next_ms_played_distance_bucket"),
        F.when(F.col("is_skipped"), "yes").otherwise("no").alias("is_skipped_label"),
    )
    .orderBy("track_id", "id")
    .limit(20)
)

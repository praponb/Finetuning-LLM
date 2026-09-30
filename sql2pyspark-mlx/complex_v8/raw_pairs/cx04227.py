from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_min_play_ms_played = Window.orderBy("min_play_ms_played", "listener_id")
w_listener_id_by_min_play_ms_p = Window.partitionBy("listener_id").orderBy("min_play_ms_played")
w_listener_id_by_max_play_ms_p = Window.partitionBy("listener_id").orderBy("max_play_ms_played", "listener_id")
w_all_by_min_play_ms_played_ti = Window.orderBy("min_play_ms_played")
w_max_play_ms_played_quartile = Window.partitionBy("max_play_ms_played_quartile", "listener_id").orderBy(F.col("min_play_ms_played_decile").desc(), "listener_id")

prepared = (
    df_plays
    .filter(F.col("listener_id").isNotNull())
    .select(
        "id",
        "listener_id",
        "track_id",
        "device",
        "is_skipped",
        "source",
        F.concat_ws("|", F.col("device"), F.col("source")).alias("device_source_label"),
        F.concat(F.col("source"), F.lit("#"), F.col("listener_id").cast("string")).alias("source_ref"),
        F.concat(F.col("device"), F.lit("#"), F.col("ms_played").cast("string")).alias("device_ref"),
    )
)

with_details_track = (
    prepared.alias("s")
    .join(df_tracks.alias("t"), (F.col("t.id") == F.col("s.track_id")) & (F.col("t.duration_sec").between(142, 357)), "inner")
    .select(
        "s.*",
        F.col("t.title").alias("track_title"),
        F.col("t.is_explicit").alias("track_is_explicit"),
    )
)

with_children = (
    with_details_track.alias("s")
    .join(df_plays.alias("p"), F.col("p.listener_id") == F.col("s.listener_id"), "inner")
    .select(
        "s.id",
        "s.listener_id",
        "s.track_id",
        "s.is_skipped",
        "s.source",
        "s.source_ref",
        "s.device_ref",
        "s.track_is_explicit",
        F.col("p.id").alias("play_id"),
        F.col("p.device").alias("play_device"),
        F.col("p.ms_played").alias("play_ms_played"),
        F.col("p.source").alias("play_source"),
    )
)

totals = (
    with_children
    .filter(F.col("play_device") == "speaker")
    .groupBy("listener_id")
    .agg(
        F.array_sort(F.collect_set("play_source")).alias("play_source_list"),
        F.min("play_ms_played").alias("min_play_ms_played"),
        F.max("play_ms_played").alias("max_play_ms_played"),
    )
)

ranked = (
    totals
    .select(
        "*",
        F.ntile(10).over(w_all_by_min_play_ms_played).alias("min_play_ms_played_decile"),
        (F.col("max_play_ms_played") - F.lag("max_play_ms_played").over(w_all_by_min_play_ms_played)).alias("max_play_ms_played_change"),
        F.round(F.percent_rank().over(w_listener_id_by_min_play_ms_p), 3).alias("min_play_ms_played_percent_rank"),
        F.lag("min_play_ms_played", 2, 0).over(w_listener_id_by_max_play_ms_p).alias("prev2_min_play_ms_played"),
        F.dense_rank().over(w_all_by_min_play_ms_played_ti).alias("min_play_ms_played_dense_rank"),
        F.ntile(4).over(w_listener_id_by_max_play_ms_p).alias("max_play_ms_played_quartile"),
    )
)

ranked_2 = (
    ranked
    .select(
        "*",
        F.row_number().over(w_max_play_ms_played_quartile).alias("min_play_ms_played_decile_seq"),
        (F.col("min_play_ms_played") - F.lag("min_play_ms_played").over(w_max_play_ms_played_quartile)).alias("min_play_ms_played_change"),
        F.lead("min_play_ms_played", 3, 0).over(w_max_play_ms_played_quartile).alias("next3_min_play_ms_played"),
    )
)

result = (
    ranked_2
    .filter(F.col("max_play_ms_played") < 261750)
    .select(
        "listener_id",
        "max_play_ms_played",
        "min_play_ms_played_decile",
        "max_play_ms_played_change",
        "min_play_ms_played_percent_rank",
        "prev2_min_play_ms_played",
        "min_play_ms_played_dense_rank",
        "max_play_ms_played_quartile",
        "min_play_ms_played_decile_seq",
        "min_play_ms_played_change",
        "next3_min_play_ms_played",
        F.least(F.col("prev2_min_play_ms_played"), F.col("min_play_ms_played")).alias("least_prev2_min_play_ms_played_min_play_ms_playe"),
        F.coalesce(F.col("next3_min_play_ms_played"), F.lit(0)).alias("next3_min_play_ms_played_filled"),
    )
    .orderBy("listener_id", F.col("min_play_ms_played_dense_rank").desc())
    .limit(20)
)

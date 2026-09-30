from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_play_played_at_ties = Window.orderBy(F.col("play_played_at").desc_nulls_first())
w_play_is_skipped_by_play_play = Window.partitionBy("play_is_skipped").orderBy("play_played_at", "id", "play_id")
w_play_is_skipped_by_play_play_2 = Window.partitionBy("play_is_skipped").orderBy("play_played_at")

recent = (
    df_plays
    .filter(F.col("source") == "search")
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "device",
        "is_skipped",
        F.date_format(F.col("played_at"), "MMM yyyy").alias("played_at_label"),
        F.coalesce(F.col("ms_played"), F.lit(0)).alias("ms_played_filled"),
        F.length(F.trim(F.col("device"))).alias("device_len"),
    )
)

exploded_detail = (
    recent.alias("s")
    .join(df_plays.alias("p"), (F.col("p.listener_id") == F.col("s.listener_id")) & (F.col("p.played_at") >= F.lit("2025-01-13 12:00:00").cast("timestamp")), "inner")
    .select(
        "s.id",
        "s.listener_id",
        "s.track_id",
        "s.device",
        "s.is_skipped",
        F.col("p.id").alias("play_id"),
        F.col("p.played_at").alias("play_played_at"),
        F.col("p.source").alias("play_source"),
        F.col("p.device").alias("play_device"),
        F.col("p.is_skipped").alias("play_is_skipped"),
    )
)

ranked = (
    exploded_detail
    .select(
        "*",
        F.round(F.cume_dist().over(w_all_by_play_played_at_ties), 3).alias("play_played_at_cume_dist"),
        F.ntile(4).over(w_play_is_skipped_by_play_play).alias("play_played_at_quartile"),
        F.ntile(3).over(w_play_is_skipped_by_play_play).alias("play_played_at_tercile"),
        F.lead("play_played_at").over(w_play_is_skipped_by_play_play).alias("next_play_played_at"),
        F.rank().over(w_play_is_skipped_by_play_play_2).alias("play_played_at_rank"),
    )
)

annotated = (
    ranked
    .select(
        "id",
        "track_id",
        "device",
        "is_skipped",
        "play_id",
        "play_device",
        "play_is_skipped",
        "play_played_at_cume_dist",
        "play_played_at_quartile",
        "play_played_at_tercile",
        "next_play_played_at",
        "play_played_at_rank",
        F.round(F.col("play_played_at_quartile") / F.nullif(F.col("play_played_at_cume_dist"), F.lit(0)), 3).alias("play_played_at_quartile_per_play_played_at_cume_"),
    )
)

result = (
    annotated
    .filter(F.col("play_played_at_rank") < 7)
    .select(
        "id",
        "device",
        "is_skipped",
        "play_id",
        "play_device",
        "play_played_at_cume_dist",
        "play_played_at_tercile",
        "next_play_played_at",
        "play_played_at_rank",
        "play_played_at_quartile_per_play_played_at_cume_",
    )
    .orderBy(F.col("play_played_at_rank").desc(), "id", "play_id")
    .limit(20)
)

from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_listener_id_track_id_by_play = Window.partitionBy("listener_id", "track_id").orderBy(F.col("played_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_played_at = Window.orderBy("played_at", "id")
w_listener_id_track_id_by_play_2 = Window.partitionBy("listener_id", "track_id").orderBy(F.col("played_at").desc(), "id")
w_all_by_played_at_full = Window.orderBy("played_at", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_ms_played_tier = Window.partitionBy("ms_played_tier")
w_ms_played_tier_by_played_at = Window.partitionBy("ms_played_tier").orderBy(F.col("played_at").asc_nulls_last(), "id")
w_ms_played_tier_by_played_at_2 = Window.partitionBy("ms_played_tier").orderBy(F.col("played_at").asc_nulls_last())

filtered_plays = (
    df_plays
    .filter(F.col("played_at") < F.lit("2025-03-02 09:00:00").cast("timestamp"))
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "ms_played",
        "device",
        "is_skipped",
        "source",
        F.when(F.col("ms_played") >= 301865, "large").when(F.col("ms_played") >= 71476, "medium").otherwise("small").alias("ms_played_tier"),
    )
)

windowed = (
    filtered_plays
    .select(
        "id",
        "track_id",
        "played_at",
        "device",
        "is_skipped",
        "source",
        F.sum("ms_played").over(w_listener_id_track_id_by_play).alias("running_ms_played"),
        F.ntile(4).over(w_all_by_played_at).alias("played_at_quartile"),
        F.row_number().over(w_listener_id_track_id_by_play_2).alias("played_at_seq"),
        F.lead("played_at", 3).over(w_listener_id_track_id_by_play_2).alias("next3_played_at"),
        F.nth_value("ms_played", 2).over(w_all_by_played_at_full).alias("second_ms_played"),
        F.max("ms_played").over(w_ms_played_tier).alias("ms_played_tier_max_ms_played"),
        F.round(F.col("ms_played") - F.avg("ms_played").over(w_ms_played_tier), 2).alias("ms_played_vs_ms_played_tier_avg"),
        F.row_number().over(w_all_by_played_at).alias("rn"),
        F.lag("ms_played", 2).over(w_all_by_played_at).alias("prev2_ms_played"),
        F.ntile(4).over(w_ms_played_tier_by_played_at).alias("played_at_quartile_2"),
        (F.col("ms_played") - F.lag("ms_played").over(w_listener_id_track_id_by_play_2)).alias("ms_played_change"),
        F.rank().over(w_ms_played_tier_by_played_at_2).alias("played_at_rank"),
    )
)

enriched = (
    windowed
    .filter(F.col("played_at_quartile_2") < 9)
    .select(
        "*",
        F.coalesce(F.col("source"), F.lit("UNKNOWN")).alias("source_or_unknown"),
    )
)

result = (
    enriched
    .select(
        "id",
        "track_id",
        "device",
        "is_skipped",
        "source",
        "running_ms_played",
        "played_at_quartile",
        "next3_played_at",
        "second_ms_played",
        "ms_played_tier_max_ms_played",
        "ms_played_change",
        "played_at_rank",
        "source_or_unknown",
        F.initcap(F.trim(F.col("source_or_unknown"))).alias("source_or_unknown_clean"),
    )
)

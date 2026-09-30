from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_listener_id_played_at_year_b = Window.partitionBy("listener_id", F.year("played_at")).orderBy(F.col("played_at").desc(), "id")
w_listener_id_played_at_year_b_2 = Window.partitionBy("listener_id", F.year("played_at")).orderBy(F.col("played_at").desc(), "id").rowsBetween(-2, Window.currentRow)
w_listener_id_is_skipped_by_pl = Window.partitionBy("listener_id", "is_skipped").orderBy("played_at", "id")
w_listener_id_played_at_year_b_3 = Window.partitionBy("listener_id", F.year("played_at")).orderBy(F.col("played_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_listener_id_is_skipped_by_pl_2 = Window.partitionBy("listener_id", "is_skipped").orderBy("played_at", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_source = Window.partitionBy("source")
w_all_by_second_played_at = Window.orderBy("second_played_at", "id")

recent_plays = (
    df_plays
    .filter((F.col("source").isin("radio", "album", "playlist")) & (F.col("track_id").isNotNull()))
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "ms_played",
        "device",
        "is_skipped",
        "source",
    )
)

ranked = (
    recent_plays
    .select(
        "id",
        "played_at",
        "ms_played",
        "source",
        F.round((F.unix_timestamp("played_at") - F.unix_timestamp(F.lag("played_at").over(w_listener_id_played_at_year_b))) / 60, 1).alias("minutes_since_prev_played_at"),
        F.sum("ms_played").over(w_listener_id_played_at_year_b_2).alias("ms_played_sum_3"),
        F.lead("ms_played").over(w_listener_id_is_skipped_by_pl).alias("next_ms_played"),
        F.last("played_at").over(w_listener_id_played_at_year_b_3).alias("last_played_at"),
        F.nth_value("played_at", 2).over(w_listener_id_is_skipped_by_pl_2).alias("second_played_at"),
    )
)

metrics = (
    ranked
    .select(
        "id",
        "ms_played",
        "source",
        "next_ms_played",
        "last_played_at",
        "second_played_at",
        F.min("ms_played_sum_3").over(w_source).alias("source_min_ms_played_sum_3"),
        (F.col("next_ms_played") - F.lag("next_ms_played").over(w_all_by_second_played_at)).alias("next_ms_played_change"),
    )
)

result = (
    metrics
    .filter(F.col("last_played_at") < F.lit("2025-03-02 10:00:00").cast("timestamp"))
    .select(
        "id",
        "ms_played",
        "source",
        "next_ms_played",
        "last_played_at",
        "second_played_at",
        "source_min_ms_played_sum_3",
        "next_ms_played_change",
    )
    .orderBy(F.col("last_played_at").desc(), "id")
    .limit(5)
)

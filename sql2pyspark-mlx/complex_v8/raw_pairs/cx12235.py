from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_track_id_source_by_played_at = Window.partitionBy("track_id", "source").orderBy(F.col("played_at").asc_nulls_last(), "id")
w_all_by_played_at = Window.orderBy(F.col("played_at").desc_nulls_first(), "id")
w_all_by_played_at_ties = Window.orderBy(F.col("played_at").desc_nulls_first())
w_all_by_played_at_running = Window.orderBy(F.col("played_at").desc_nulls_first(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_track_id_source_by_played_at_2 = Window.partitionBy("track_id", "source").orderBy(F.col("played_at").asc_nulls_last())
w_all_by_played_at_2 = Window.orderBy("played_at", "id")
w_source_listener_id_by_played = Window.partitionBy("source", "listener_id").orderBy(F.col("played_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_listener_id_by_played_at_tie = Window.partitionBy("listener_id").orderBy("played_at")

cleaned = (
    df_plays
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "device",
        "is_skipped",
        "source",
        F.lower(F.regexp_replace(F.trim(F.col("device")), " ", "_")).alias("device_slug"),
        F.coalesce(F.col("device"), F.lit("UNKNOWN")).alias("device_or_unknown"),
    )
)

sequenced = (
    cleaned
    .select(
        "id",
        "played_at",
        "source",
        "device_slug",
        "device_or_unknown",
        F.round((F.unix_timestamp("played_at") - F.unix_timestamp(F.lag("played_at").over(w_track_id_source_by_played_at))) / 60, 1).alias("minutes_since_prev_played_at"),
        F.lead("played_at", 3).over(w_all_by_played_at).alias("next3_played_at"),
        F.round(F.percent_rank().over(w_all_by_played_at_ties), 3).alias("played_at_percent_rank"),
        F.first("played_at", ignorenulls=True).over(w_all_by_played_at_running).alias("first_known_played_at"),
        F.round(F.percent_rank().over(w_track_id_source_by_played_at_2), 3).alias("played_at_percent_rank_2"),
        F.round((F.unix_timestamp("played_at") - F.unix_timestamp(F.lag("played_at").over(w_all_by_played_at_2))) / 60, 1).alias("minutes_since_prev_played_at_2"),
        F.lead("played_at").over(w_track_id_source_by_played_at).alias("next_played_at"),
        F.first("played_at").over(w_source_listener_id_by_played).alias("first_played_at"),
        F.round(F.cume_dist().over(w_listener_id_by_played_at_tie), 3).alias("played_at_cume_dist"),
    )
)

result = (
    sequenced
    .select(
        "id",
        "played_at",
        "source",
        "device_slug",
        "device_or_unknown",
        "next3_played_at",
        "played_at_percent_rank",
        "first_known_played_at",
        "played_at_percent_rank_2",
        "minutes_since_prev_played_at_2",
        "next_played_at",
        "first_played_at",
        "played_at_cume_dist",
        F.least(F.col("played_at_percent_rank_2"), F.col("played_at_percent_rank")).alias("least_played_at_percent_rank_2_played_at_percent"),
        F.initcap(F.trim(F.col("device_or_unknown"))).alias("device_or_unknown_clean"),
    )
    .orderBy(F.col("next_played_at").desc())
)

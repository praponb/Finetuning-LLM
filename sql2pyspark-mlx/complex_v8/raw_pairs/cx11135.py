from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_device_track_id_by_played_at = Window.partitionBy("device", "track_id").orderBy(F.col("played_at").desc(), "id")
w_track_id_by_played_at = Window.partitionBy("track_id").orderBy("played_at", "id")
w_all_by_played_at_full = Window.orderBy("played_at", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_device_by_played_at = Window.partitionBy("device").orderBy(F.col("played_at").desc(), "id")

recent = (
    df_plays
    .filter(F.col("track_id").isNotNull())
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "device",
        "is_skipped",
        "source",
        F.date_format(F.col("played_at"), "MMM yyyy").alias("played_at_label"),
    )
)

sequenced = (
    recent
    .select(
        "*",
        F.dense_rank().over(w_device_track_id_by_played_at).alias("played_at_dense_rank"),
        F.rank().over(w_device_track_id_by_played_at).alias("played_at_rank"),
        F.round(F.percent_rank().over(w_track_id_by_played_at), 3).alias("played_at_percent_rank"),
        F.ntile(10).over(w_track_id_by_played_at).alias("played_at_decile"),
        F.lag("played_at", 2).over(w_track_id_by_played_at).alias("prev2_played_at"),
        F.first("played_at").over(w_all_by_played_at_full).alias("first_played_at"),
        F.lead("played_at").over(w_track_id_by_played_at).alias("next_played_at"),
        F.round((F.unix_timestamp("played_at") - F.unix_timestamp(F.lag("played_at").over(w_track_id_by_played_at))) / 60, 1).alias("minutes_since_prev_played_at"),
        F.lag("played_at").over(w_device_by_played_at).alias("prev_played_at"),
        F.last("played_at").over(w_all_by_played_at_full).alias("last_played_at"),
        F.ntile(5).over(w_device_track_id_by_played_at).alias("played_at_quintile"),
    )
)

result = (
    sequenced
    .select(
        "id",
        "listener_id",
        "is_skipped",
        "source",
        "played_at_dense_rank",
        "played_at_rank",
        "played_at_percent_rank",
        "played_at_decile",
        "prev2_played_at",
        "first_played_at",
        "next_played_at",
        "minutes_since_prev_played_at",
        "prev_played_at",
        F.concat(F.col("source"), F.lit("#"), F.col("played_at_dense_rank").cast("string")).alias("source_ref"),
        F.datediff(F.lit("2026-01-01").cast("date"), F.col("next_played_at")).alias("days_since_next_played_at"),
    )
    .orderBy(F.col("first_played_at").desc())
)

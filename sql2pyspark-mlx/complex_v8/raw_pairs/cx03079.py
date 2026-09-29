from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_is_skipped_by_played_at_date = Window.partitionBy("is_skipped").orderBy(F.col("played_at_date").desc_nulls_first(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_is_skipped_by_played_at_date_2 = Window.partitionBy("is_skipped").orderBy(F.col("played_at_date").desc_nulls_first(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_is_skipped_by_played_at_date_3 = Window.partitionBy("is_skipped").orderBy(F.col("played_at_date").desc_nulls_first())
w_listener_id = Window.partitionBy("listener_id")
w_is_skipped_by_played_at_date_4 = Window.partitionBy("is_skipped").orderBy(F.col("played_at_date").desc_nulls_first(), "id")

base = (
    df_plays
    .filter(F.col("source") == "radio")
    .select(
        "id",
        "listener_id",
        "track_id",
        "ms_played",
        "device",
        "is_skipped",
        "source",
        F.to_date(F.col("played_at")).alias("played_at_date"),
        (F.floor(F.col("ms_played") / 50000) * 50000).alias("ms_played_bucket"),
        F.coalesce(F.col("ms_played"), F.lit(0)).alias("ms_played_filled"),
    )
)

with_windows = (
    base
    .select(
        "*",
        F.first("ms_played", ignorenulls=True).over(w_is_skipped_by_played_at_date).alias("first_known_ms_played"),
        F.nth_value("played_at_date", 2).over(w_is_skipped_by_played_at_date_2).alias("second_played_at_date"),
        F.rank().over(w_is_skipped_by_played_at_date_3).alias("played_at_date_rank"),
        F.round(F.col("ms_played_filled") / F.nullif(F.sum("ms_played_filled").over(w_listener_id), F.lit(0)), 4).alias("ms_played_filled_share_of_listener_id"),
        F.rank().over(w_is_skipped_by_played_at_date_4).alias("played_at_date_rank_2"),
        F.lag("ms_played_filled", 1, 0).over(w_is_skipped_by_played_at_date_4).alias("prev_ms_played_filled"),
    )
)

annotated = (
    with_windows
    .select(
        "id",
        "track_id",
        "device",
        "source",
        "played_at_date",
        "ms_played_filled",
        "first_known_ms_played",
        "played_at_date_rank",
        "ms_played_filled_share_of_listener_id",
        "played_at_date_rank_2",
        F.abs(F.col("ms_played_bucket") - 123500).alias("ms_played_bucket_distance"),
    )
)

enriched = (
    annotated
    .select(
        "id",
        "track_id",
        "device",
        "ms_played_filled",
        "first_known_ms_played",
        "played_at_date_rank",
        "ms_played_filled_share_of_listener_id",
        "played_at_date_rank_2",
        "ms_played_bucket_distance",
        F.round(F.col("first_known_ms_played") / F.nullif(F.col("played_at_date_rank_2"), F.lit(0)), 3).alias("first_known_ms_played_per_played_at_date_rank_2"),
        F.coalesce(F.col("source"), F.lit("UNKNOWN")).alias("source_or_unknown"),
    )
)

result = (
    enriched
    .groupBy("played_at_date_rank", "device")
    .pivot("source_or_unknown", ["album", "UNKNOWN", "radio"])
    .agg(F.max("first_known_ms_played_per_played_at_date_rank_2"))
    .orderBy("played_at_date_rank", "device")
)

from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_is_skipped_by_ms_played_dist = Window.partitionBy("is_skipped").orderBy(F.col("ms_played_distance").desc())
w_is_skipped_by_ms_played_dist_2 = Window.partitionBy("is_skipped").orderBy(F.col("ms_played_distance").desc(), "id")
w_is_skipped_by_ms_played_dist_3 = Window.partitionBy("is_skipped").orderBy(F.col("ms_played_distance").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

filtered_plays = (
    df_plays
    .filter((F.col("device").isin("car", "speaker", "tv")) & (F.col("ms_played") < 287769))
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "ms_played",
        "device",
        "is_skipped",
        "source",
        F.coalesce(F.col("ms_played"), F.lit(0)).alias("ms_played_filled"),
        F.abs(F.col("ms_played") - 221269).alias("ms_played_distance"),
    )
)

annotated = (
    filtered_plays
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "ms_played",
        "device",
        "is_skipped",
        "ms_played_distance",
        F.when(F.col("is_skipped"), "yes").otherwise("no").alias("is_skipped_label"),
    )
)

windowed = (
    annotated
    .select(
        "*",
        F.round(F.percent_rank().over(w_is_skipped_by_ms_played_dist), 3).alias("ms_played_distance_percent_rank"),
        F.round(F.percent_rank().over(w_is_skipped_by_ms_played_dist_2), 3).alias("ms_played_distance_percent_rank_2"),
        F.min("ms_played").over(w_is_skipped_by_ms_played_dist_3).alias("min_ms_played_so_far"),
    )
)

result = (
    windowed
    .select(
        "id",
        "listener_id",
        "track_id",
        "ms_played",
        "device",
        "ms_played_distance",
        "is_skipped_label",
        "ms_played_distance_percent_rank",
        F.concat_ws("-", F.col("device"), F.col("is_skipped_label")).alias("device_is_skipped_label_label"),
        F.length(F.trim(F.col("device"))).alias("device_len"),
    )
    .orderBy(F.col("id").desc(), "track_id")
)

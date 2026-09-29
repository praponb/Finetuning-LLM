from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_ms_played_ties = Window.orderBy(F.col("ms_played").desc())
w_device_ms_played_tier = Window.partitionBy("device", "ms_played_tier")
w_source_track_id_by_played_at = Window.partitionBy("source", "track_id").orderBy("played_at", "id")
w_all_by_ms_played = Window.orderBy(F.col("ms_played").desc(), "id")
w_source_track_id_by_played_at_2 = Window.partitionBy("source", "track_id").orderBy("played_at", "id").rowsBetween(-2, Window.currentRow)
w_all_by_ms_played_full = Window.orderBy(F.col("ms_played").desc_nulls_first(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

prepared_plays = (
    df_plays
    .filter((F.month(F.col("played_at")).isin(1, 2)) & (F.col("track_id") >= 8))
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "ms_played",
        "device",
        "is_skipped",
        "source",
        F.when(F.col("ms_played") >= 225733, "large").when(F.col("ms_played") >= 129336, "medium").otherwise("small").alias("ms_played_tier"),
        F.concat(F.col("device"), F.lit("#"), F.col("listener_id").cast("string")).alias("device_ref"),
    )
)

with_windows = (
    prepared_plays
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "ms_played",
        "device",
        "is_skipped",
        F.round(F.percent_rank().over(w_all_by_ms_played_ties), 3).alias("ms_played_percent_rank"),
        F.size(F.collect_set("source").over(w_device_ms_played_tier)).alias("distinct_source_in_device_ms_played_tier"),
        F.round(F.col("ms_played") / F.nullif(F.sum("ms_played").over(w_device_ms_played_tier), F.lit(0)), 4).alias("ms_played_share_of_device_ms_played_tier"),
        F.max("ms_played").over(w_device_ms_played_tier).alias("device_ms_played_tier_max_ms_played"),
        F.row_number().over(w_source_track_id_by_played_at).alias("played_at_seq"),
        F.lead("played_at", 2).over(w_all_by_ms_played).alias("next2_played_at"),
        F.round(F.avg("ms_played").over(w_source_track_id_by_played_at_2), 2).alias("ms_played_avg_3"),
        F.count(F.lit(1)).over(w_device_ms_played_tier).alias("rows_in_device_ms_played_tier"),
        F.rank().over(w_all_by_ms_played).alias("ms_played_rank"),
        F.first("played_at").over(w_all_by_ms_played_full).alias("first_played_at"),
    )
)

result = (
    with_windows
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "device",
        "is_skipped",
        "ms_played_percent_rank",
        "ms_played_share_of_device_ms_played_tier",
        "device_ms_played_tier_max_ms_played",
        "next2_played_at",
        "ms_played_avg_3",
        "rows_in_device_ms_played_tier",
        "ms_played_rank",
        F.to_date(F.col("first_played_at")).alias("first_played_at_date"),
    )
    .orderBy(F.col("ms_played_rank").asc())
)

from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_device_by_played_at_running = Window.partitionBy("device").orderBy(F.col("played_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_source_by_played_at_running = Window.partitionBy("source").orderBy(F.col("played_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_device = Window.partitionBy("device")
w_track_id_played_at_month = Window.partitionBy("track_id", F.month("played_at"))
w_listener_id_device_by_ms_pla = Window.partitionBy("listener_id", "device").orderBy(F.col("ms_played").desc_nulls_first(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_track_id_by_played_at_moving = Window.partitionBy("track_id").orderBy("played_at", "id").rowsBetween(-1, Window.currentRow)
w_listener_id_device_by_ms_pla_2 = Window.partitionBy("listener_id", "device").orderBy(F.col("ms_played").desc_nulls_first())
w_source_by_played_at_ties = Window.partitionBy("source").orderBy(F.col("played_at").desc())
w_is_skipped_source = Window.partitionBy("is_skipped", "source")
w_device_by_played_at = Window.partitionBy("device").orderBy(F.col("played_at").desc(), "id")
w_listener_id_device_by_ms_pla_3 = Window.partitionBy("listener_id", "device").orderBy(F.col("ms_played").desc_nulls_first(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_source_by_played_at_full = Window.partitionBy("source").orderBy("played_at", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_is_skipped_by_played_at_cume = Window.partitionBy("is_skipped").orderBy(F.col("played_at_cume_dist").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_second_played_at_full = Window.orderBy("second_played_at", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_second_played_at = Window.orderBy("second_played_at", "id")
w_is_skipped = Window.partitionBy("is_skipped")

scoped = (
    df_plays
    .filter(F.col("device").isin("tv", "car", "phone"))
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

sequenced = (
    scoped
    .select(
        "id",
        "track_id",
        "played_at",
        "ms_played",
        "is_skipped",
        "source",
        F.first("ms_played", ignorenulls=True).over(w_device_by_played_at_running).alias("first_known_ms_played"),
        F.last("ms_played", ignorenulls=True).over(w_source_by_played_at_running).alias("last_known_ms_played"),
        F.round(F.col("ms_played") - F.avg("ms_played").over(w_device), 2).alias("ms_played_vs_device_avg"),
        F.round(F.col("ms_played") - F.avg("ms_played").over(w_track_id_played_at_month), 2).alias("ms_played_vs_track_id_played_at_month_avg"),
        F.nth_value("played_at", 2).over(w_listener_id_device_by_ms_pla).alias("second_played_at"),
        F.max("ms_played").over(w_track_id_by_played_at_moving).alias("ms_played_max_2"),
        F.round(F.cume_dist().over(w_listener_id_device_by_ms_pla_2), 3).alias("ms_played_cume_dist"),
        F.round(F.cume_dist().over(w_source_by_played_at_ties), 3).alias("played_at_cume_dist"),
        F.round(F.col("ms_played") / F.nullif(F.sum("ms_played").over(w_is_skipped_source), F.lit(0)), 4).alias("ms_played_share_of_is_skipped_source"),
        F.row_number().over(w_device_by_played_at).alias("seq_no"),
        F.last("ms_played", ignorenulls=True).over(w_listener_id_device_by_ms_pla_3).alias("last_known_ms_played_2"),
    )
)

leaders = (
    sequenced
    .filter((F.col("seq_no") <= 2) & (F.col("first_known_ms_played") >= 52009))
    .drop("seq_no")
)

ranked = (
    leaders
    .select(
        "*",
        F.nth_value("ms_played", 2).over(w_source_by_played_at_full).alias("second_ms_played"),
        F.nth_value("ms_played_cume_dist", 2).over(w_is_skipped_by_played_at_cume).alias("second_ms_played_cume_dist"),
        F.last("ms_played_vs_device_avg").over(w_all_by_second_played_at_full).alias("last_ms_played_vs_device_avg"),
        F.lead("ms_played_max_2").over(w_all_by_second_played_at).alias("next_ms_played_max_2"),
        F.count(F.lit(1)).over(w_is_skipped).alias("rows_in_is_skipped"),
        F.round(F.col("played_at_cume_dist") / F.nullif(F.sum("played_at_cume_dist").over(w_is_skipped), F.lit(0)), 4).alias("played_at_cume_dist_share_of_is_skipped"),
    )
)

result = (
    ranked
    .select(
        "id",
        "track_id",
        "played_at",
        "ms_played",
        "source",
        "ms_played_vs_track_id_played_at_month_avg",
        "ms_played_max_2",
        "ms_played_cume_dist",
        "played_at_cume_dist",
        "ms_played_share_of_is_skipped_source",
        "second_ms_played",
        "second_ms_played_cume_dist",
        "last_ms_played_vs_device_avg",
        "next_ms_played_max_2",
        "rows_in_is_skipped",
        "played_at_cume_dist_share_of_is_skipped",
        F.when(F.col("ms_played") >= 221269, "large").when(F.col("ms_played") >= 73335, "medium").otherwise("small").alias("ms_played_tier"),
        (F.floor(F.col("next_ms_played_max_2") / 1000000) * 1000000).alias("next_ms_played_max_2_bucket"),
    )
    .orderBy(F.col("ms_played").desc())
)

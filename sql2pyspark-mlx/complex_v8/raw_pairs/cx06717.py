from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_listener_id_source = Window.partitionBy("listener_id", "source")
w_source_by_played_at = Window.partitionBy("source").orderBy("played_at", "id")
w_played_at_year_listener_id_b = Window.partitionBy(F.year("played_at"), "listener_id").orderBy(F.col("played_at").desc(), "id")
w_is_skipped_by_played_at_movi = Window.partitionBy("is_skipped").orderBy(F.col("played_at").desc(), "id").rowsBetween(-3, Window.currentRow)
w_is_skipped_device = Window.partitionBy("is_skipped", "device")
w_played_at_year_listener_id_b_2 = Window.partitionBy(F.year("played_at"), "listener_id").orderBy(F.col("played_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_is_skipped_by_played_at = Window.partitionBy("is_skipped").orderBy(F.col("played_at").desc(), "id")
w_all_by_ms_played_ties = Window.orderBy(F.col("ms_played").desc())

recent = (
    df_plays
    .filter((F.col("is_skipped")) & (F.col("ms_played") >= 71476))
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

windowed = (
    recent
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "ms_played",
        "device",
        "source",
        F.size(F.collect_set("track_id").over(w_listener_id_source)).alias("distinct_track_id_in_listener_id_source"),
        F.lag("ms_played").over(w_source_by_played_at).alias("prev_ms_played"),
        F.ntile(4).over(w_played_at_year_listener_id_b).alias("played_at_quartile"),
        (F.col("ms_played") - F.lag("ms_played").over(w_played_at_year_listener_id_b)).alias("ms_played_change"),
        F.round(F.avg("ms_played").over(w_is_skipped_by_played_at_movi), 2).alias("ms_played_avg_4"),
        F.min("ms_played").over(w_is_skipped_device).alias("is_skipped_device_min_ms_played"),
        F.last("ms_played", ignorenulls=True).over(w_played_at_year_listener_id_b_2).alias("last_known_ms_played"),
        F.lag("ms_played", 3).over(w_is_skipped_by_played_at).alias("prev3_ms_played"),
        F.round(F.col("ms_played") / F.nullif(F.sum("ms_played").over(w_is_skipped_device), F.lit(0)), 4).alias("ms_played_share_of_is_skipped_device"),
        F.lead("ms_played").over(w_played_at_year_listener_id_b).alias("next_ms_played"),
        F.rank().over(w_all_by_ms_played_ties).alias("ms_played_rank"),
        F.sum("ms_played").over(w_is_skipped_device).alias("is_skipped_device_sum_ms_played"),
        F.max("ms_played").over(w_is_skipped_device).alias("is_skipped_device_max_ms_played"),
    )
)

result = (
    windowed
    .select(
        "id",
        "played_at",
        "ms_played",
        "source",
        "distinct_track_id_in_listener_id_source",
        "prev_ms_played",
        "played_at_quartile",
        "ms_played_change",
        "ms_played_avg_4",
        "is_skipped_device_min_ms_played",
        "last_known_ms_played",
        "prev3_ms_played",
        "ms_played_rank",
        F.initcap(F.trim(F.col("device"))).alias("device_clean"),
    )
    .orderBy(F.col("device_clean").asc())
)

from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_is_skipped_by_played_at = Window.partitionBy("is_skipped").orderBy(F.col("played_at").desc(), "id")
w_listener_id_by_ms_played_dis = Window.partitionBy("listener_id").orderBy(F.col("ms_played_distance").desc(), "id")
w_played_at_year_is_skipped = Window.partitionBy(F.year("played_at"), "is_skipped")
w_listener_id_by_ms_played_dis_2 = Window.partitionBy("listener_id").orderBy(F.col("ms_played_distance").desc(), "id").rowsBetween(-6, Window.currentRow)
w_track_id_played_at_to_date_b = Window.partitionBy("track_id", F.to_date("played_at")).orderBy("played_at", "id")
w_track_id_device = Window.partitionBy("track_id", "device")
w_track_id_played_at_to_date_b_2 = Window.partitionBy("track_id", F.to_date("played_at")).orderBy("played_at", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_track_id_played_at_to_date_b_3 = Window.partitionBy("track_id", F.to_date("played_at")).orderBy("played_at")
w_is_skipped_by_played_at_ties = Window.partitionBy("is_skipped").orderBy(F.col("played_at").desc())
w_played_at_month_source = Window.partitionBy(F.date_trunc("month", "played_at"), "source")
w_source_track_id_by_ms_played = Window.partitionBy("source", "track_id").orderBy("ms_played_distance")
w_source_track_id_by_ms_played_2 = Window.partitionBy("source", "track_id").orderBy("ms_played_distance", "id")
w_track_id_by_ms_played_distan = Window.partitionBy("track_id").orderBy(F.col("ms_played_distance_rank").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_next2_played_at_full = Window.orderBy(F.col("next2_played_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_device = Window.partitionBy("device")

prepared = (
    df_plays
    .filter((F.col("played_at") < F.lit("2025-03-02 09:00:00").cast("timestamp")) & (F.col("device") == "desktop"))
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "device",
        "is_skipped",
        "source",
        F.abs(F.col("ms_played") - 130888).alias("ms_played_distance"),
    )
)

with_trends = (
    prepared
    .select(
        "*",
        F.row_number().over(w_is_skipped_by_played_at).alias("seq_no"),
        F.ntile(5).over(w_listener_id_by_ms_played_dis).alias("ms_played_distance_quintile"),
        F.count(F.lit(1)).over(w_played_at_year_is_skipped).alias("rows_in_played_at_year_is_skipped"),
        F.max("ms_played_distance").over(w_listener_id_by_ms_played_dis_2).alias("ms_played_distance_max_7"),
        F.lead("played_at", 2).over(w_track_id_played_at_to_date_b).alias("next2_played_at"),
        F.size(F.collect_set("listener_id").over(w_played_at_year_is_skipped)).alias("distinct_listener_id_in_played_at_year_is_skippe"),
        F.size(F.collect_set("device").over(w_played_at_year_is_skipped)).alias("distinct_device_in_played_at_year_is_skipped"),
        F.round(F.col("ms_played_distance") / F.nullif(F.sum("ms_played_distance").over(w_track_id_device), F.lit(0)), 4).alias("ms_played_distance_share_of_track_id_device"),
        F.min("ms_played_distance").over(w_track_id_device).alias("track_id_device_min_ms_played_distance"),
        F.first("played_at").over(w_track_id_played_at_to_date_b_2).alias("first_played_at"),
        F.dense_rank().over(w_track_id_played_at_to_date_b_3).alias("played_at_dense_rank"),
        F.dense_rank().over(w_is_skipped_by_played_at_ties).alias("played_at_dense_rank_2"),
        F.max("ms_played_distance").over(w_played_at_month_source).alias("played_at_month_source_max_ms_played_distance"),
        F.rank().over(w_source_track_id_by_ms_played).alias("ms_played_distance_rank"),
        F.lag("played_at", 3).over(w_source_track_id_by_ms_played_2).alias("prev3_played_at"),
    )
)

sequenced = (
    with_trends
    .select(
        "*",
        F.last("played_at", ignorenulls=True).over(w_track_id_by_ms_played_distan).alias("last_known_played_at"),
        F.nth_value("ms_played_distance_max_7", 2).over(w_all_by_next2_played_at_full).alias("second_ms_played_distance_max_7"),
        F.count(F.lit(1)).over(w_device).alias("rows_in_device"),
        F.round(F.col("seq_no") / F.nullif(F.sum("seq_no").over(w_device), F.lit(0)), 4).alias("seq_no_share_of_device"),
        F.size(F.collect_set("track_id").over(w_device)).alias("distinct_track_id_in_device"),
    )
)

result = (
    sequenced
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "device",
        "is_skipped",
        "source",
        "ms_played_distance",
        "seq_no",
        "ms_played_distance_quintile",
        "rows_in_played_at_year_is_skipped",
        "ms_played_distance_max_7",
        "next2_played_at",
        "distinct_listener_id_in_played_at_year_is_skippe",
        "ms_played_distance_share_of_track_id_device",
        "first_played_at",
        "played_at_dense_rank",
        "played_at_dense_rank_2",
        "played_at_month_source_max_ms_played_distance",
        "ms_played_distance_rank",
        "second_ms_played_distance_max_7",
        "seq_no_share_of_device",
        F.concat(F.col("source"), F.lit("#"), F.col("ms_played_distance_max_7").cast("string")).alias("source_ref"),
    )
)

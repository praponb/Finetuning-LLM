from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_track_duration_sec_ru = Window.orderBy("track_duration_sec", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_is_skipped = Window.partitionBy("is_skipped")
w_all_by_track_duration_sec = Window.orderBy("track_duration_sec", "id")
w_device_played_at_dayofweek_b = Window.partitionBy("device", "played_at_dayofweek").orderBy("played_at_dayofweek", "id")
w_track_artist_id_by_played_at = Window.partitionBy("track_artist_id").orderBy("played_at_dayofweek", "id")
w_device_played_at_dayofweek_b_2 = Window.partitionBy("device", "played_at_dayofweek").orderBy("played_at_dayofweek")
w_device_played_at_dayofweek_b_3 = Window.partitionBy("device", "played_at_dayofweek").orderBy("played_at_dayofweek", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_device_track_id = Window.partitionBy("device", "track_id")
w_all_by_track_duration_sec_ti = Window.orderBy("track_duration_sec")
w_all_by_rows_in_device_track = Window.orderBy(F.col("rows_in_device_track_id").desc(), "id")
w_all_by_rows_in_device_track_2 = Window.orderBy(F.col("rows_in_device_track_id").desc())
w_track_artist_id = Window.partitionBy("track_artist_id")
w_first_played_at_dayofweek_tr = Window.partitionBy("first_played_at_dayofweek", "track_artist_id").orderBy(F.col("rn_2").asc_nulls_last(), "id")

valid = (
    df_plays
    .filter((F.col("track_id").isNotNull()) & (~F.col("is_skipped")))
    .select(
        "id",
        "listener_id",
        "track_id",
        "device",
        "is_skipped",
        "source",
        F.concat_ws(": ", F.col("device"), F.col("source")).alias("device_source_label"),
        F.dayofweek(F.col("played_at")).alias("played_at_dayofweek"),
    )
)

enriched_track = (
    valid.alias("s")
    .join(df_tracks.alias("t"), F.col("t.id") == F.col("s.track_id"), "inner")
    .select(
        "s.id",
        "s.listener_id",
        "s.track_id",
        "s.device",
        "s.is_skipped",
        "s.device_source_label",
        "s.played_at_dayofweek",
        F.col("t.bpm").alias("track_bpm"),
        F.col("t.artist_id").alias("track_artist_id"),
        F.col("t.duration_sec").alias("track_duration_sec"),
    )
)

metrics = (
    enriched_track
    .select(
        "id",
        "listener_id",
        "track_id",
        "device",
        "device_source_label",
        "track_bpm",
        "track_artist_id",
        F.last("played_at_dayofweek", ignorenulls=True).over(w_all_by_track_duration_sec_ru).alias("last_known_played_at_dayofweek"),
        F.sum("played_at_dayofweek").over(w_is_skipped).alias("is_skipped_sum_played_at_dayofweek"),
        F.row_number().over(w_all_by_track_duration_sec).alias("rn"),
        F.lead("track_duration_sec").over(w_device_played_at_dayofweek_b).alias("next_track_duration_sec"),
        F.row_number().over(w_track_artist_id_by_played_at).alias("rn_2"),
        F.dense_rank().over(w_device_played_at_dayofweek_b_2).alias("played_at_dayofweek_dense_rank"),
        F.first("played_at_dayofweek").over(w_device_played_at_dayofweek_b_3).alias("first_played_at_dayofweek"),
        F.count(F.lit(1)).over(w_device_track_id).alias("rows_in_device_track_id"),
        F.dense_rank().over(w_all_by_track_duration_sec_ti).alias("track_duration_sec_dense_rank"),
        F.size(F.collect_set("device").over(w_is_skipped)).alias("distinct_device_in_is_skipped"),
        F.last("track_bpm").over(w_device_played_at_dayofweek_b_3).alias("last_track_bpm"),
        F.lead("played_at_dayofweek", 1, 0).over(w_track_artist_id_by_played_at).alias("next_played_at_dayofweek"),
    )
)

first_rows = (
    metrics
    .filter((F.col("rn") == 1) & (F.col("distinct_device_in_is_skipped").between(2, 8)))
    .drop("rn")
)

metrics_2 = (
    first_rows
    .select(
        "*",
        F.round(F.percent_rank().over(w_all_by_rows_in_device_track), 3).alias("rows_in_device_track_id_percent_rank"),
        F.round(F.cume_dist().over(w_all_by_rows_in_device_track_2), 3).alias("rows_in_device_track_id_cume_dist"),
        F.round(F.col("next_played_at_dayofweek") - F.avg("next_played_at_dayofweek").over(w_track_artist_id), 2).alias("next_played_at_dayofweek_vs_track_artist_id_avg"),
        F.count(F.lit(1)).over(w_track_artist_id).alias("rows_in_track_artist_id"),
        F.max("rn_2").over(w_track_artist_id).alias("track_artist_id_max_rn_2"),
        F.ntile(3).over(w_first_played_at_dayofweek_tr).alias("rn_2_tercile"),
    )
)

totals = (
    metrics_2
    .groupBy(
        "track_duration_sec_dense_rank",
        "last_known_played_at_dayofweek",
        "first_played_at_dayofweek",
    )
    .agg(
        F.round(F.sum("rows_in_device_track_id_cume_dist"), 2).alias("total_rows_in_device_track_id_cume_dist"),
        F.countDistinct("track_artist_id").alias("distinct_track_artist_ids"),
        F.sum(F.when(~F.col("device").isin("tv", "speaker"), F.col("rows_in_device_track_id")).otherwise(0)).alias("rows_in_device_track_id_device_not_in_tv_speaker"),
        F.array_sort(F.collect_set("device")).alias("device_list"),
    )
)

result = (
    totals
    .select(
        "track_duration_sec_dense_rank",
        "first_played_at_dayofweek",
        "device_list",
    )
    .orderBy(
        F.col("track_duration_sec_dense_rank").desc_nulls_first(),
        "first_played_at_dayofweek",
    )
)

from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_track_duration_sec_pr = Window.orderBy("track_duration_sec", "id").rowsBetween(-6, -1)
w_device_is_skipped_by_track_d = Window.partitionBy("device", "is_skipped").orderBy("track_duration_sec")
w_all_by_track_duration_sec_ru = Window.orderBy("track_duration_sec", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_track_is_explicit = Window.partitionBy("track_is_explicit")
w_track_id_by_track_duration_s = Window.partitionBy("track_id").orderBy("track_duration_sec", "id")
w_track_id_by_track_duration_s_2 = Window.partitionBy("track_id").orderBy("track_duration_sec", "id").rowsBetween(-3, -1)
w_track_id_by_track_duration_s_3 = Window.partitionBy("track_id").orderBy("track_duration_sec")
w_track_id_by_track_duration_s_4 = Window.partitionBy("track_id").orderBy("track_duration_sec", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_played_at_to_date_device_by = Window.partitionBy(F.to_date("played_at"), "device").orderBy(F.col("track_duration_sec_sum_3_prior").desc(), "id")
w_track_duration_sec_dense_ran = Window.partitionBy("track_duration_sec_dense_rank_2", "device").orderBy("track_duration_sec_dense_rank_2", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_track_is_explicit_is_skipped = Window.partitionBy("track_is_explicit", "is_skipped").orderBy("track_duration_sec_dense_rank_2", "id").rowsBetween(-6, Window.currentRow)

prepared = (
    df_plays
    .filter((F.col("track_id").isNotNull()) & (F.col("ms_played") <= 340114))
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "device",
        "is_skipped",
    )
)

combined_details_track = (
    prepared.alias("s")
    .join(df_tracks.alias("t"), (F.col("t.id") == F.col("s.track_id")) & (~F.col("t.is_explicit")), "inner")
    .filter(F.col("t.duration_sec") >= 144)
    .select(
        "s.id",
        "s.listener_id",
        "s.track_id",
        "s.played_at",
        "s.device",
        "s.is_skipped",
        F.col("t.is_explicit").alias("track_is_explicit"),
        F.col("t.duration_sec").alias("track_duration_sec"),
    )
)

metrics = (
    combined_details_track
    .select(
        "*",
        F.round(F.avg("track_duration_sec").over(w_all_by_track_duration_sec_pr), 2).alias("track_duration_sec_avg_6_prior"),
        F.dense_rank().over(w_device_is_skipped_by_track_d).alias("track_duration_sec_dense_rank"),
        F.sum("track_duration_sec").over(w_all_by_track_duration_sec_ru).alias("running_track_duration_sec"),
        F.min("track_duration_sec").over(w_track_is_explicit).alias("track_is_explicit_min_track_duration_sec"),
        (F.col("track_duration_sec") - F.lag("track_duration_sec").over(w_track_id_by_track_duration_s)).alias("track_duration_sec_change"),
        F.sum("track_duration_sec").over(w_track_id_by_track_duration_s_2).alias("track_duration_sec_sum_3_prior"),
        F.dense_rank().over(w_track_id_by_track_duration_s_3).alias("track_duration_sec_dense_rank_2"),
        F.last("track_duration_sec").over(w_track_id_by_track_duration_s_4).alias("last_track_duration_sec"),
    )
)

scored = (
    metrics
    .select(
        "*",
        F.when(F.col("device").isin("desktop", "tv"), "group_a").when(F.col("device") == "phone", "group_b").otherwise("other").alias("device_group"),
        (F.floor(F.col("last_track_duration_sec") / 50) * 50).alias("last_track_duration_sec_bucket"),
    )
)

sequenced = (
    scored
    .select(
        "*",
        (F.col("running_track_duration_sec") - F.lag("running_track_duration_sec").over(w_played_at_to_date_device_by)).alias("running_track_duration_sec_change"),
        F.min("last_track_duration_sec").over(w_track_duration_sec_dense_ran).alias("min_last_track_duration_sec_so_far"),
        F.sum("last_track_duration_sec").over(w_track_is_explicit_is_skipped).alias("last_track_duration_sec_sum_7"),
    )
)

totals = (
    sequenced
    .groupBy(
        "listener_id",
        "track_duration_sec_dense_rank_2",
        F.when(F.col("last_track_duration_sec") >= 352, "HIGH").when(F.col("last_track_duration_sec") >= 154, "MEDIUM").otherwise("LOW").alias("last_track_duration_sec_tier"),
    )
    .agg(
        F.countDistinct("device_group").alias("distinct_device_groups"),
        F.min("running_track_duration_sec_change").alias("min_running_track_duration_sec_change"),
        F.sum(F.when(F.col("device") == "phone", F.col("min_last_track_duration_sec_so_far"))).alias("min_last_track_duration_sec_so_far_device_phone"),
    )
)

result = (
    totals
    .select(
        "listener_id",
        "last_track_duration_sec_tier",
        "distinct_device_groups",
        "min_last_track_duration_sec_so_far_device_phone",
    )
    .orderBy(
        F.col("distinct_device_groups").asc(),
        F.col("listener_id").desc_nulls_first(),
    )
)

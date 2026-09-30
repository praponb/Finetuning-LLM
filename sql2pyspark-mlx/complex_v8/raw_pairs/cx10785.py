from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_listener_plan_by_ms_played_b = Window.partitionBy("listener_plan").orderBy("ms_played_bucket", "id")
w_is_skipped_by_ms_played_buck = Window.partitionBy("is_skipped").orderBy("ms_played_bucket", "id")
w_listener_id_is_skipped_by_tr = Window.partitionBy("listener_id", "is_skipped").orderBy(F.col("track_release_date").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_is_skipped_by_ms_played_buck_2 = Window.partitionBy("is_skipped").orderBy("ms_played_bucket")
w_listener_plan_by_ms_played_b_2 = Window.partitionBy("listener_plan").orderBy("ms_played_bucket", "id").rowsBetween(-6, -1)
w_listener_id_is_skipped_by_tr_2 = Window.partitionBy("listener_id", "is_skipped").orderBy(F.col("track_release_date").asc_nulls_last(), "id")
w_listener_plan_by_ms_played_b_3 = Window.partitionBy("listener_plan").orderBy("ms_played_bucket", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_listener_id = Window.partitionBy("listener_id")
w_listener_id_is_skipped_by_tr_3 = Window.partitionBy("listener_id", "is_skipped").orderBy(F.col("track_release_date").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_is_skipped_by_track_release = Window.partitionBy("is_skipped").orderBy("track_release_date", "id").rowsBetween(-3, Window.currentRow)
w_device = Window.partitionBy("device")
w_listener_id_is_skipped_by_tr_4 = Window.partitionBy("listener_id", "is_skipped").orderBy(F.col("track_release_date").asc_nulls_last())
w_is_skipped_by_ms_played_buck_3 = Window.partitionBy("is_skipped").orderBy("ms_played_bucket", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_device_is_skipped_by_ms_play = Window.partitionBy("device", "is_skipped").orderBy("ms_played_bucket_quintile", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_listener_id_ms_played_bucket = Window.partitionBy("listener_id", "ms_played_bucket_quintile").orderBy(F.col("second_ms_played_bucket").desc_nulls_first(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_track_release_date_quartile = Window.partitionBy("track_release_date_quartile").orderBy("greatest_ms_played_bucket_max_4_second_ms_played", "id")
w_listener_id_ms_played_bucket_2 = Window.partitionBy("listener_id", "ms_played_bucket_quintile").orderBy(F.col("second_ms_played_bucket").desc_nulls_first(), "id")
w_device_is_skipped_by_ms_play_2 = Window.partitionBy("device", "is_skipped").orderBy("ms_played_bucket_quintile", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_listener_id_ms_played_bucket_3 = Window.partitionBy("listener_id", "ms_played_bucket_quintile").orderBy(F.col("second_ms_played_bucket").desc_nulls_first())
w_is_skipped_prev_track_releas = Window.partitionBy("is_skipped", F.date_trunc("month", "prev_track_release_date"))
w_track_release_date_quartile_2 = Window.partitionBy("track_release_date_quartile", "ms_played_bucket_quintile").orderBy(F.col("last_track_release_date").desc(), "ms_played_bucket_quintile", "greatest_ms_played_bucket_max_4_second_ms_played_2", "track_release_date_quartile").rowsBetween(-3, -1)
w_greatest_ms_played_bucket_ma = Window.partitionBy("greatest_ms_played_bucket_max_4_second_ms_played_2").orderBy(F.col("max_last_ms_played_bucket").desc(), "ms_played_bucket_quintile", "greatest_ms_played_bucket_max_4_second_ms_played_2", "track_release_date_quartile").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

base = (
    df_plays
    .filter((F.col("listener_id") >= 13) | (F.col("ms_played") <= 239955))
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "device",
        "is_skipped",
        "source",
        (F.floor(F.col("ms_played") / 50000) * 50000).alias("ms_played_bucket"),
    )
)

with_details = (
    base.alias("s")
    .join(df_listeners.alias("l"), F.col("l.id") == F.col("s.listener_id"), "inner")
    .join(df_tracks.alias("t"), F.col("t.id") == F.col("s.track_id"), "left")
    .select(
        "s.id",
        "s.listener_id",
        "s.track_id",
        "s.device",
        "s.is_skipped",
        "s.ms_played_bucket",
        F.col("l.username").alias("listener_username"),
        F.col("l.interests").alias("listener_interests"),
        F.col("l.plan").alias("listener_plan"),
        F.col("t.title").alias("track_title"),
        F.col("t.release_date").alias("track_release_date"),
    )
)

windowed = (
    with_details
    .select(
        "*",
        F.lag("track_release_date").over(w_listener_plan_by_ms_played_b).alias("prev_track_release_date"),
        F.ntile(5).over(w_is_skipped_by_ms_played_buck).alias("ms_played_bucket_quintile"),
        F.last("ms_played_bucket").over(w_listener_id_is_skipped_by_tr).alias("last_ms_played_bucket"),
        F.round(F.percent_rank().over(w_is_skipped_by_ms_played_buck_2), 3).alias("ms_played_bucket_percent_rank"),
        F.sum("ms_played_bucket").over(w_listener_plan_by_ms_played_b_2).alias("ms_played_bucket_sum_6_prior"),
        F.ntile(4).over(w_listener_id_is_skipped_by_tr_2).alias("track_release_date_quartile"),
        F.nth_value("ms_played_bucket", 2).over(w_listener_plan_by_ms_played_b_3).alias("second_ms_played_bucket"),
        F.max("ms_played_bucket").over(w_listener_id).alias("listener_id_max_ms_played_bucket"),
        F.last("ms_played_bucket", ignorenulls=True).over(w_listener_id_is_skipped_by_tr_3).alias("last_known_ms_played_bucket"),
        F.max("ms_played_bucket").over(w_is_skipped_by_track_release).alias("ms_played_bucket_max_4"),
        F.round(F.col("ms_played_bucket") / F.nullif(F.sum("ms_played_bucket").over(w_listener_id), F.lit(0)), 4).alias("ms_played_bucket_share_of_listener_id"),
        F.sum("ms_played_bucket").over(w_listener_id).alias("listener_id_sum_ms_played_bucket"),
        F.round(F.avg("ms_played_bucket").over(w_device), 2).alias("device_avg_ms_played_bucket"),
        F.round(F.cume_dist().over(w_listener_id_is_skipped_by_tr_4), 3).alias("track_release_date_cume_dist"),
        F.nth_value("track_release_date", 2).over(w_is_skipped_by_ms_played_buck_3).alias("second_track_release_date"),
    )
)

tagged = (
    windowed
    .filter(F.col("track_release_date_cume_dist") >= 0.2)
    .select(
        "*",
        F.substring(F.trim(F.col("listener_plan")), 1, 2).alias("listener_plan_prefix"),
        F.greatest(F.col("ms_played_bucket_max_4"), F.col("second_ms_played_bucket")).alias("greatest_ms_played_bucket_max_4_second_ms_played"),
    )
)

with_trends = (
    tagged
    .select(
        "id",
        "listener_id",
        "track_id",
        "device",
        "is_skipped",
        "ms_played_bucket",
        "listener_username",
        "listener_interests",
        "listener_plan",
        "track_title",
        "track_release_date",
        "ms_played_bucket_quintile",
        "last_ms_played_bucket",
        "ms_played_bucket_percent_rank",
        "ms_played_bucket_sum_6_prior",
        "track_release_date_quartile",
        "second_ms_played_bucket",
        "last_known_ms_played_bucket",
        "ms_played_bucket_max_4",
        "ms_played_bucket_share_of_listener_id",
        "listener_id_sum_ms_played_bucket",
        "track_release_date_cume_dist",
        "second_track_release_date",
        "listener_plan_prefix",
        "greatest_ms_played_bucket_max_4_second_ms_played",
        F.first("ms_played_bucket_sum_6_prior").over(w_device_is_skipped_by_ms_play).alias("first_ms_played_bucket_sum_6_prior"),
        F.last("ms_played_bucket_quintile", ignorenulls=True).over(w_listener_id_ms_played_bucket).alias("last_known_ms_played_bucket_quintile"),
        F.ntile(3).over(w_track_release_date_quartile).alias("greatest_ms_played_bucket_max_4_second_ms_played_2"),
        (F.col("ms_played_bucket_quintile") - F.lag("ms_played_bucket_quintile").over(w_listener_id_ms_played_bucket_2)).alias("ms_played_bucket_quintile_change"),
        F.first("second_ms_played_bucket", ignorenulls=True).over(w_device_is_skipped_by_ms_play_2).alias("first_known_second_ms_played_bucket"),
        F.rank().over(w_listener_id_ms_played_bucket_3).alias("second_ms_played_bucket_rank"),
        F.round(F.avg("ms_played_bucket_max_4").over(w_is_skipped_prev_track_releas), 2).alias("is_skipped_prev_track_release_date_month_avg_ms_"),
    )
)

totals = (
    with_trends
    .groupBy(
        "ms_played_bucket_quintile",
        "greatest_ms_played_bucket_max_4_second_ms_played_2",
        "track_release_date_quartile",
    )
    .agg(
        F.round(F.stddev("greatest_ms_played_bucket_max_4_second_ms_played"), 2).alias("stddev_greatest_ms_played_bucket_max_4_second_ms"),
        F.sum("track_release_date_quartile").alias("total_track_release_date_quartile"),
        F.max("last_ms_played_bucket").alias("max_last_ms_played_bucket"),
        F.round(F.avg("track_release_date_cume_dist"), 2).alias("avg_track_release_date_cume_dist"),
        F.max("track_release_date").alias("last_track_release_date"),
    )
    .filter(F.col("max_last_ms_played_bucket") > 123500)
)

with_windows = (
    totals
    .select(
        "ms_played_bucket_quintile",
        "greatest_ms_played_bucket_max_4_second_ms_played_2",
        "track_release_date_quartile",
        "total_track_release_date_quartile",
        "max_last_ms_played_bucket",
        "avg_track_release_date_cume_dist",
        "last_track_release_date",
        F.max("max_last_ms_played_bucket").over(w_track_release_date_quartile_2).alias("max_last_ms_played_bucket_max_3_prior"),
        F.first("last_track_release_date").over(w_greatest_ms_played_bucket_ma).alias("first_last_track_release_date"),
    )
)

result = (
    with_windows
    .select(
        "ms_played_bucket_quintile",
        "greatest_ms_played_bucket_max_4_second_ms_played_2",
        "total_track_release_date_quartile",
        "max_last_ms_played_bucket",
        "avg_track_release_date_cume_dist",
        "last_track_release_date",
        "max_last_ms_played_bucket_max_3_prior",
        "first_last_track_release_date",
    )
    .orderBy(F.col("greatest_ms_played_bucket_max_4_second_ms_played_2").desc())
)

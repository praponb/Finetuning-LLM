from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_listener_id_distinct_devices = Window.partitionBy("listener_id", "distinct_devices").orderBy("first_played_at", "listener_id").rowsBetween(-2, Window.currentRow)
w_listener_id = Window.partitionBy("listener_id")
w_distinct_devices_listener_id = Window.partitionBy("distinct_devices", "listener_id").orderBy(F.col("total_ms_played").desc_nulls_first(), "listener_id").rowsBetween(-6, -1)
w_listener_id_distinct_devices_2 = Window.partitionBy("listener_id", "distinct_devices").orderBy("first_played_at", "listener_id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

filtered_plays = df_plays.filter((~F.col("is_skipped")) | (F.col("track_id") <= 20))

per_group = (
    filtered_plays
    .filter(F.col("ms_played") >= 95960)
    .groupBy("listener_id")
    .agg(
        F.count("*").alias("cnt"),
        F.sum("ms_played").alias("total_ms_played"),
        F.min("played_at").alias("first_played_at"),
        F.countDistinct("device").alias("distinct_devices"),
    )
)

without_matches = (
    per_group.alias("s")
    .join(df_listeners.alias("l"), (F.col("l.id") == F.col("s.listener_id")) & (F.col("l.country").isNotNull()) & (~F.col("l.plan").isin("free", "student")), "left_semi")
)

metrics = (
    without_matches
    .select(
        "*",
        F.round(F.avg("total_ms_played").over(w_listener_id_distinct_devices), 2).alias("total_ms_played_avg_3"),
        F.round(F.avg("cnt").over(w_distinct_devices_listener_id), 2).alias("cnt_avg_6_prior"),
        F.nth_value("total_ms_played", 2).over(w_listener_id_distinct_devices_2).alias("second_total_ms_played"),
        F.sum("total_ms_played").over(w_listener_id).alias("listener_id_sum_total_ms_played"),
    )
)

result = (
    metrics
    .filter(F.col("listener_id_sum_total_ms_played") < 16801500)
    .select(
        "listener_id",
        "total_ms_played",
        "distinct_devices",
        "total_ms_played_avg_3",
        "cnt_avg_6_prior",
        "second_total_ms_played",
        F.round(F.col("cnt_avg_6_prior") * 0.25, 2).alias("cnt_avg_6_prior_adj"),
        (F.floor(F.col("second_total_ms_played") / 1000000) * 1000000).alias("second_total_ms_played_bucket"),
    )
)

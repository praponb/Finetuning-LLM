from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_track_id = Window.partitionBy("track_id")
w_played_at_year_by_ms_played = Window.partitionBy(F.year("played_at")).orderBy("ms_played", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_played_at_year_by_ms_played_2 = Window.partitionBy(F.year("played_at")).orderBy("ms_played")
w_listener_id_played_at_month = Window.partitionBy("listener_id", F.date_trunc("month", "played_at"))
w_track_release_date_year_by_l = Window.partitionBy(F.year("track_release_date")).orderBy(F.col("listener_birth_year").desc(), "id")
w_source_listener_id = Window.partitionBy("source", "listener_id")
w_all_by_listener_birth_year = Window.orderBy("listener_birth_year", "id")
w_source_distinct_track_id_in = Window.partitionBy("source", "distinct_track_id_in_source_listener_id")
w_distinct_listener_id_in_trac = Window.partitionBy("distinct_listener_id_in_track_id").orderBy(F.col("played_at").desc(), "id")
w_distinct_listener_id_in_trac_2 = Window.partitionBy("distinct_listener_id_in_track_id").orderBy(F.col("played_at").desc(), "id").rowsBetween(-2, -1)
w_played_at_day_month_device_b = Window.partitionBy(F.month("played_at_day"), "device").orderBy("avg_ms_played_percent_rank_avg_2_prior", "played_at_day", "device")
w_played_at_day_month_device_b_2 = Window.partitionBy(F.month("played_at_day"), "device").orderBy("avg_ms_played_percent_rank_avg_2_prior")

valid = df_plays.filter(F.col("track_id") <= 24)

joined = (
    valid.alias("s")
    .join(df_listeners.alias("l"), F.col("l.id") == F.col("s.listener_id"), "inner")
    .join(df_tracks.alias("t"), F.col("t.id") == F.col("s.track_id"), "inner")
    .filter(F.col("l.birth_year") <= 1981)
    .select(
        "s.id",
        "s.listener_id",
        "s.track_id",
        "s.played_at",
        "s.ms_played",
        "s.device",
        "s.source",
        F.col("l.birth_year").alias("listener_birth_year"),
        F.col("t.release_date").alias("track_release_date"),
    )
)

metrics = (
    joined
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "device",
        "source",
        "listener_birth_year",
        F.size(F.collect_set("listener_id").over(w_track_id)).alias("distinct_listener_id_in_track_id"),
        F.min("listener_birth_year").over(w_played_at_year_by_ms_played).alias("min_listener_birth_year_so_far"),
        F.round(F.percent_rank().over(w_played_at_year_by_ms_played_2), 3).alias("ms_played_percent_rank"),
        F.min("listener_birth_year").over(w_listener_id_played_at_month).alias("listener_id_played_at_month_min_listener_birth_y"),
        F.dense_rank().over(w_track_release_date_year_by_l).alias("listener_birth_year_dense_rank"),
        F.size(F.collect_set("track_id").over(w_source_listener_id)).alias("distinct_track_id_in_source_listener_id"),
        F.lag("played_at", 2).over(w_all_by_listener_birth_year).alias("prev2_played_at"),
    )
)

leaders = (
    metrics
    .filter(F.col("listener_birth_year_dense_rank") == 1)
    .drop("listener_birth_year_dense_rank")
)

sequenced = (
    leaders
    .select(
        "id",
        "track_id",
        "played_at",
        "device",
        "source",
        "distinct_listener_id_in_track_id",
        "min_listener_birth_year_so_far",
        "ms_played_percent_rank",
        "listener_id_played_at_month_min_listener_birth_y",
        "distinct_track_id_in_source_listener_id",
        F.sum("listener_birth_year").over(w_source_distinct_track_id_in).alias("source_distinct_track_id_in_source_listener_id_s"),
        F.round(F.percent_rank().over(w_distinct_listener_id_in_trac), 3).alias("played_at_percent_rank"),
        F.lag("listener_birth_year", 3, 0).over(w_distinct_listener_id_in_trac).alias("prev3_listener_birth_year"),
        F.size(F.collect_set("track_id").over(w_source_distinct_track_id_in)).alias("distinct_track_id_in_source_distinct_track_id_in"),
        F.round(F.avg("ms_played_percent_rank").over(w_distinct_listener_id_in_trac_2), 2).alias("ms_played_percent_rank_avg_2_prior"),
        F.count(F.lit(1)).over(w_source_distinct_track_id_in).alias("rows_in_source_distinct_track_id_in_source_liste"),
    )
)

stats = (
    sequenced
    .groupBy(F.to_date("played_at").alias("played_at_day"), "device")
    .agg(
        F.max("min_listener_birth_year_so_far").alias("max_min_listener_birth_year_so_far"),
        F.array_sort(F.collect_set("source")).alias("source_list"),
        F.round(F.avg("ms_played_percent_rank_avg_2_prior"), 2).alias("avg_ms_played_percent_rank_avg_2_prior"),
    )
)

with_windows = (
    stats
    .select(
        "*",
        F.round(F.cume_dist().over(w_played_at_day_month_device_b), 3).alias("avg_ms_played_percent_rank_avg_2_prior_cume_dist"),
        F.lag("played_at_day", 3).over(w_played_at_day_month_device_b).alias("prev3_played_at_day"),
        F.round(F.percent_rank().over(w_played_at_day_month_device_b_2), 3).alias("avg_ms_played_percent_rank_avg_2_prior_percent_r"),
    )
)

result = (
    with_windows
    .filter(F.size(F.col("source_list")) >= 1)
    .select(
        "played_at_day",
        "device",
        "max_min_listener_birth_year_so_far",
        "source_list",
        "avg_ms_played_percent_rank_avg_2_prior",
        "avg_ms_played_percent_rank_avg_2_prior_cume_dist",
        "prev3_played_at_day",
        F.when(F.col("device").isin("desktop", "tv"), "group_a").when(F.col("device") == "phone", "group_b").otherwise("other").alias("device_group"),
    )
    .orderBy("prev3_played_at_day", "device_group")
)

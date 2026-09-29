from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_listener_id_is_skipped = Window.partitionBy("listener_id", "is_skipped")
w_device_source_by_played_at_f = Window.partitionBy("device", "source").orderBy(F.col("played_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_track_id_first_played_at_mon = Window.partitionBy("track_id", F.year("first_played_at_month"))
w_distinct_track_id_in_listene = Window.partitionBy("distinct_track_id_in_listener_id_is_skipped", "track_id").orderBy("n_device_is_not_null", "distinct_track_id_in_listener_id_is_skipped", "track_id", "first_played_at_month").rowsBetween(Window.unboundedPreceding, Window.currentRow)

recent = (
    df_plays
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "ms_played",
        "device",
        "is_skipped",
        "source",
        F.date_trunc("year", F.col("played_at")).alias("played_at_year"),
    )
)

with_trends = (
    recent
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "device",
        "is_skipped",
        "source",
        F.size(F.collect_set("track_id").over(w_listener_id_is_skipped)).alias("distinct_track_id_in_listener_id_is_skipped"),
        F.count(F.lit(1)).over(w_listener_id_is_skipped).alias("rows_in_listener_id_is_skipped"),
        F.first("played_at").over(w_device_source_by_played_at_f).alias("first_played_at"),
        F.round(F.avg("ms_played").over(w_listener_id_is_skipped), 2).alias("listener_id_is_skipped_avg_ms_played"),
    )
)

with_details = (
    with_trends.alias("s")
    .join(df_tracks.alias("t"), (F.col("t.id") == F.col("s.track_id")) & (~F.col("t.is_explicit")), "inner")
    .filter(F.size(F.col("t.moods")) >= 2)
    .select("s.*", F.col("t.moods").alias("track_moods"))
)

summary = (
    with_details
    .filter(F.col("device").isNotNull())
    .groupBy(
        "distinct_track_id_in_listener_id_is_skipped",
        "track_id",
        F.date_trunc("month", "first_played_at").alias("first_played_at_month"),
    )
    .agg(
        F.count(F.when(F.col("device").isNotNull(), 1)).alias("n_device_is_not_null"),
        F.round(100 * F.count(F.when(F.col("device") == "desktop", 1)) / F.count("*"), 1).alias("pct_device_desktop"),
    )
)

sequenced = (
    summary
    .select(
        "*",
        F.max("n_device_is_not_null").over(w_track_id_first_played_at_mon).alias("track_id_first_played_at_month_year_max_n_device"),
        F.round(F.col("pct_device_desktop") - F.avg("pct_device_desktop").over(w_track_id_first_played_at_mon), 2).alias("pct_device_desktop_vs_track_id_first_played_at_m"),
        F.sum("n_device_is_not_null").over(w_track_id_first_played_at_mon).alias("track_id_first_played_at_month_year_sum_n_device"),
        F.count(F.lit(1)).over(w_track_id_first_played_at_mon).alias("rows_in_track_id_first_played_at_month_year"),
        F.count(F.lit(1)).over(w_distinct_track_id_in_listene).alias("running_count"),
    )
)

tagged = (
    sequenced
    .select(
        "*",
        F.coalesce(F.col("running_count"), F.lit(0)).alias("running_count_filled"),
        F.round(F.col("pct_device_desktop") * 0.9, 2).alias("pct_device_desktop_adj"),
        F.date_trunc("hour", F.col("first_played_at_month")).alias("first_played_at_month_hour"),
    )
)

result = (
    tagged
    .filter(F.col("rows_in_track_id_first_played_at_month_year") >= 9)
    .select(
        "distinct_track_id_in_listener_id_is_skipped",
        "first_played_at_month",
        "n_device_is_not_null",
        "pct_device_desktop",
        "track_id_first_played_at_month_year_max_n_device",
        "pct_device_desktop_vs_track_id_first_played_at_m",
        "track_id_first_played_at_month_year_sum_n_device",
        "rows_in_track_id_first_played_at_month_year",
        "running_count",
        "running_count_filled",
        "pct_device_desktop_adj",
        "first_played_at_month_hour",
        F.dayofweek(F.col("first_played_at_month")).alias("first_played_at_month_dayofweek"),
        F.round(F.col("pct_device_desktop_adj") * 1.5, 2).alias("pct_device_desktop_adj_adj"),
    )
    .orderBy("pct_device_desktop_adj")
)

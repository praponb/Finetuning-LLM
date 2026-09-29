from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_played_at_plus_30m_ti = Window.orderBy("played_at_plus_30m")
w_all_by_played_at_plus_30m_fu = Window.orderBy("played_at_plus_30m", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_played_at_plus_30m_ru = Window.orderBy("played_at_plus_30m", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_track_id_by_played_at_plus_3 = Window.partitionBy("track_id").orderBy(F.col("played_at_plus_30m").desc(), "id")
w_listener_id_by_played_at_plu = Window.partitionBy("listener_id").orderBy(F.col("played_at_plus_30m").desc_nulls_first())
w_listener_id_by_played_at_plu_2 = Window.partitionBy("listener_id").orderBy(F.col("played_at_plus_30m").desc_nulls_first(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_played_at_plus_30m = Window.orderBy("played_at_plus_30m", "id")
w_listener_id_by_played_at_plu_3 = Window.partitionBy("listener_id").orderBy(F.col("played_at_plus_30m").desc_nulls_first(), "id")
w_track_id_by_played_at_plus_3_2 = Window.partitionBy("track_id").orderBy(F.col("played_at_plus_30m").asc_nulls_last(), "id")
w_track_id_by_played_at_plus_3_3 = Window.partitionBy("track_id").orderBy(F.col("played_at_plus_30m").asc_nulls_last())
w_listener_id_by_played_at_plu_4 = Window.partitionBy("listener_id").orderBy(F.col("played_at_plus_30m").desc(), "id")
w_played_at_plus_30m_seq_by_pl = Window.partitionBy("played_at_plus_30m_seq").orderBy(F.col("played_at_plus_30m").desc_nulls_first())
w_prev_played_at_plus_30m_mont = Window.partitionBy(F.date_trunc("month", "prev_played_at_plus_30m"))

cleaned = (
    df_plays
    .filter(F.col("played_at") >= F.lit("2025-01-13 14:00:00").cast("timestamp"))
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "device",
        "is_skipped",
        "source",
        (F.col("played_at") + F.expr("INTERVAL 30 MINUTES")).alias("played_at_plus_30m"),
        F.substring(F.trim(F.col("source")), 1, 3).alias("source_prefix"),
    )
)

with_details_listener = (
    cleaned.alias("s")
    .join(df_listeners.alias("l"), F.col("l.id") == F.col("s.listener_id"), "inner")
    .select(
        "s.id",
        "s.listener_id",
        "s.track_id",
        "s.device",
        "s.played_at_plus_30m",
        "s.source_prefix",
        F.col("l.username").alias("listener_username"),
    )
)

windowed = (
    with_details_listener
    .select(
        "*",
        F.rank().over(w_all_by_played_at_plus_30m_ti).alias("played_at_plus_30m_rank"),
        F.last("played_at_plus_30m").over(w_all_by_played_at_plus_30m_fu).alias("last_played_at_plus_30m"),
        F.first("played_at_plus_30m", ignorenulls=True).over(w_all_by_played_at_plus_30m_ru).alias("first_known_played_at_plus_30m"),
        F.lag("played_at_plus_30m").over(w_track_id_by_played_at_plus_3).alias("prev_played_at_plus_30m"),
        F.dense_rank().over(w_listener_id_by_played_at_plu).alias("played_at_plus_30m_dense_rank"),
        F.first("played_at_plus_30m", ignorenulls=True).over(w_listener_id_by_played_at_plu_2).alias("first_known_played_at_plus_30m_2"),
        F.lead("played_at_plus_30m", 2).over(w_all_by_played_at_plus_30m).alias("next2_played_at_plus_30m"),
        F.ntile(4).over(w_listener_id_by_played_at_plu_3).alias("played_at_plus_30m_quartile"),
        F.ntile(4).over(w_track_id_by_played_at_plus_3_2).alias("played_at_plus_30m_quartile_2"),
        F.last("played_at_plus_30m", ignorenulls=True).over(w_all_by_played_at_plus_30m_ru).alias("last_known_played_at_plus_30m"),
        F.dense_rank().over(w_track_id_by_played_at_plus_3_3).alias("played_at_plus_30m_dense_rank_2"),
        F.row_number().over(w_listener_id_by_played_at_plu_4).alias("played_at_plus_30m_seq"),
        F.round((F.unix_timestamp("played_at_plus_30m") - F.unix_timestamp(F.lag("played_at_plus_30m").over(w_listener_id_by_played_at_plu_3))) / 60, 1).alias("minutes_since_prev_played_at_plus_30m"),
        F.lag("played_at_plus_30m").over(w_listener_id_by_played_at_plu_4).alias("prev_played_at_plus_30m_2"),
    )
)

first_rows = (
    windowed
    .filter(F.col("played_at_plus_30m_dense_rank") <= 5)
    .drop("played_at_plus_30m_dense_rank")
)

with_trends = (
    first_rows
    .select(
        "*",
        F.round(F.percent_rank().over(w_played_at_plus_30m_seq_by_pl), 3).alias("played_at_plus_30m_percent_rank"),
        F.size(F.collect_set("device").over(w_prev_played_at_plus_30m_mont)).alias("distinct_device_in_prev_played_at_plus_30m_month"),
        F.count(F.lit(1)).over(w_prev_played_at_plus_30m_mont).alias("rows_in_prev_played_at_plus_30m_month"),
        F.max("played_at_plus_30m_rank").over(w_prev_played_at_plus_30m_mont).alias("prev_played_at_plus_30m_month_max_played_at_plus"),
    )
)

result = (
    with_trends
    .select(
        "id",
        "listener_id",
        "device",
        "source_prefix",
        "last_played_at_plus_30m",
        "first_known_played_at_plus_30m",
        "prev_played_at_plus_30m",
        "first_known_played_at_plus_30m_2",
        "next2_played_at_plus_30m",
        "played_at_plus_30m_quartile_2",
        "last_known_played_at_plus_30m",
        "played_at_plus_30m_dense_rank_2",
        "played_at_plus_30m_seq",
        "minutes_since_prev_played_at_plus_30m",
        "rows_in_prev_played_at_plus_30m_month",
        "prev_played_at_plus_30m_month_max_played_at_plus",
    )
    .orderBy("source_prefix", "played_at_plus_30m_seq")
)

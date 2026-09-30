from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_played_at_year_by_played_at = Window.partitionBy(F.year("played_at")).orderBy("played_at", "id")
w_track_id_is_skipped = Window.partitionBy("track_id", "is_skipped")
w_played_at_year_month_track_i = Window.partitionBy(F.date_trunc("month", "played_at_year"), "track_id").orderBy(F.col("ms_played").asc_nulls_last())
w_listener_id_is_skipped = Window.partitionBy("listener_id", "is_skipped")
w_listener_id_by_ms_played = Window.partitionBy("listener_id").orderBy(F.col("ms_played").desc(), "id")
w_played_at_year_by_played_at_2 = Window.partitionBy(F.year("played_at")).orderBy("played_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_played_at = Window.orderBy("played_at", "id")
w_listener_id_by_ms_played_run = Window.partitionBy("listener_id").orderBy(F.col("ms_played").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_last_known_played_at_year_2 = Window.partitionBy(F.to_date("last_known_played_at_year_2"))
w_all_by_played_at_year_full = Window.orderBy("played_at_year", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_played_at_year_ties = Window.orderBy("played_at_year")

scoped_plays = (
    df_plays
    .filter(F.col("device").isin("phone", "tv", "speaker"))
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "ms_played",
        "device",
        "is_skipped",
        F.date_trunc("year", F.col("played_at")).alias("played_at_year"),
    )
)

with_parent_listener = (
    scoped_plays.alias("s")
    .join(df_listeners.alias("l"), F.col("l.id") == F.col("s.listener_id"), "inner")
    .select(
        "s.id",
        "s.listener_id",
        "s.track_id",
        "s.played_at",
        "s.ms_played",
        "s.device",
        "s.is_skipped",
        "s.played_at_year",
        F.col("l.birth_year").alias("listener_birth_year"),
        F.col("l.signup_date").alias("listener_signup_date"),
    )
)

ranked = (
    with_parent_listener
    .select(
        "*",
        F.lead("played_at_year").over(w_played_at_year_by_played_at).alias("next_played_at_year"),
        F.round(F.col("listener_birth_year") / F.nullif(F.sum("listener_birth_year").over(w_track_id_is_skipped), F.lit(0)), 4).alias("listener_birth_year_share_of_track_id_is_skipped"),
        F.round(F.percent_rank().over(w_played_at_year_month_track_i), 3).alias("ms_played_percent_rank"),
        F.count(F.lit(1)).over(w_listener_id_is_skipped).alias("rows_in_listener_id_is_skipped"),
        F.row_number().over(w_played_at_year_by_played_at).alias("played_at_seq"),
        F.ntile(5).over(w_listener_id_by_ms_played).alias("ms_played_quintile"),
        F.lead("listener_birth_year", 2).over(w_listener_id_by_ms_played).alias("next2_listener_birth_year"),
        F.round(F.percent_rank().over(w_listener_id_by_ms_played), 3).alias("ms_played_percent_rank_2"),
        F.last("played_at_year", ignorenulls=True).over(w_played_at_year_by_played_at_2).alias("last_known_played_at_year"),
        F.lag("ms_played", 2).over(w_all_by_played_at).alias("prev2_ms_played"),
        F.sum("listener_birth_year").over(w_track_id_is_skipped).alias("track_id_is_skipped_sum_listener_birth_year"),
        F.round(F.col("listener_birth_year") - F.avg("listener_birth_year").over(w_track_id_is_skipped), 2).alias("listener_birth_year_vs_track_id_is_skipped_avg"),
        F.last("played_at_year", ignorenulls=True).over(w_listener_id_by_ms_played_run).alias("last_known_played_at_year_2"),
    )
)

leaders = ranked.filter(F.col("played_at_seq") <= 5)

metrics = (
    leaders
    .select(
        "*",
        F.min("ms_played_quintile").over(w_last_known_played_at_year_2).alias("last_known_played_at_year_2_to_date_min_ms_playe"),
        F.nth_value("ms_played_percent_rank_2", 2).over(w_all_by_played_at_year_full).alias("second_ms_played_percent_rank_2"),
        F.min("played_at_seq").over(w_last_known_played_at_year_2).alias("last_known_played_at_year_2_to_date_min_played_a"),
        F.round(F.cume_dist().over(w_all_by_played_at_year_ties), 3).alias("played_at_year_cume_dist"),
        F.first("ms_played_quintile").over(w_all_by_played_at_year_full).alias("first_ms_played_quintile"),
    )
)

summary = (
    metrics
    .groupBy("played_at_seq")
    .agg(
        F.count(F.when(F.col("device").isin("desktop", "tv", "car"), 1)).alias("n_device_in_desktop_tv_car"),
        F.sum("listener_birth_year_share_of_track_id_is_skipped").alias("total_listener_birth_year_share_of_track_id_is_s"),
        F.min("ms_played_percent_rank_2").alias("min_ms_played_percent_rank_2"),
        F.datediff(F.max("listener_signup_date"), F.min("listener_signup_date")).alias("listener_signup_date_span_days"),
        F.round(F.avg("prev2_ms_played"), 2).alias("avg_prev2_ms_played"),
        F.avg("prev2_ms_played").alias("avg_prev2_ms_played_check"),
    )
    .filter(F.col("avg_prev2_ms_played_check") > 71476)
    .drop("avg_prev2_ms_played_check")
)

result = (
    summary
    .select(
        "played_at_seq",
        "min_ms_played_percent_rank_2",
        "avg_prev2_ms_played",
        F.when(F.col("played_at_seq") >= 7, "HIGH").when(F.col("played_at_seq") >= 3, "MEDIUM").otherwise("LOW").alias("played_at_seq_tier"),
    )
    .orderBy(F.col("avg_prev2_ms_played").desc(), "played_at_seq")
)

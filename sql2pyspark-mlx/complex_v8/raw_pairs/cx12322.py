from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_source_listener_signup_date = Window.partitionBy("source", F.month("listener_signup_date")).orderBy(F.col("played_at_quarter").desc())
w_source_played_at_to_date_by = Window.partitionBy("source", F.to_date("played_at")).orderBy("listener_signup_date", "id")
w_is_skipped_track_id = Window.partitionBy("is_skipped", "track_id")
w_all_by_played_at_quarter_tie = Window.orderBy("played_at_quarter")
w_all_by_played_at_quarter_ful = Window.orderBy("played_at_quarter", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_played_at_quarter = Window.orderBy("played_at_quarter", "id")
w_played_at_quarter_month_is_s = Window.partitionBy(F.month("played_at_quarter"), "is_skipped")
w_all_by_played_at_full = Window.orderBy(F.col("played_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_source_listener_signup_date_2 = Window.partitionBy("source", "listener_signup_date_quintile").orderBy(F.col("listener_birth_year_vs_is_skipped_track_id_avg").desc(), "id").rowsBetween(-2, -1)
w_source_listener_signup_date_3 = Window.partitionBy("source", "listener_signup_date_quintile").orderBy(F.col("listener_birth_year_vs_is_skipped_track_id_avg").desc(), "id")
w_all_by_played_at_moving_2 = Window.orderBy(F.col("played_at").desc(), "id").rowsBetween(-2, Window.currentRow)

cleaned = (
    df_plays
    .filter((F.col("ms_played") >= 120663) & (F.col("source") == "search"))
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "ms_played",
        "is_skipped",
        "source",
        F.date_trunc("quarter", F.col("played_at")).alias("played_at_quarter"),
        F.concat_ws("|", F.col("device"), F.col("source")).alias("device_source_label"),
    )
)

combined_details_listener = (
    cleaned.alias("s")
    .join(df_listeners.alias("l"), F.col("l.id") == F.col("s.listener_id"), "left")
    .select(
        "s.id",
        "s.listener_id",
        "s.track_id",
        "s.played_at",
        "s.is_skipped",
        "s.source",
        "s.played_at_quarter",
        "s.device_source_label",
        F.col("l.signup_date").alias("listener_signup_date"),
        F.col("l.birth_year").alias("listener_birth_year"),
        F.col("l.interests").alias("listener_interests"),
    )
)

ranked = (
    combined_details_listener
    .select(
        "*",
        F.dense_rank().over(w_source_listener_signup_date).alias("played_at_quarter_dense_rank"),
        F.ntile(5).over(w_source_played_at_to_date_by).alias("listener_signup_date_quintile"),
        F.round(F.col("listener_birth_year") - F.avg("listener_birth_year").over(w_is_skipped_track_id), 2).alias("listener_birth_year_vs_is_skipped_track_id_avg"),
        F.round(F.cume_dist().over(w_all_by_played_at_quarter_tie), 3).alias("played_at_quarter_cume_dist"),
        F.nth_value("listener_signup_date", 2).over(w_all_by_played_at_quarter_ful).alias("second_listener_signup_date"),
        F.lead("played_at_quarter").over(w_source_played_at_to_date_by).alias("next_played_at_quarter"),
        F.lead("listener_signup_date").over(w_all_by_played_at_quarter).alias("next_listener_signup_date"),
        F.min("listener_birth_year").over(w_played_at_quarter_month_is_s).alias("played_at_quarter_month_is_skipped_min_listener_"),
        F.row_number().over(w_all_by_played_at_quarter).alias("played_at_quarter_seq"),
    )
)

top_per_group = (
    ranked
    .filter((F.col("played_at_quarter_dense_rank") <= 5) & (F.col("is_skipped")))
    .drop("played_at_quarter_dense_rank")
)

with_windows = (
    top_per_group
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "played_at_quarter",
        "device_source_label",
        "listener_birth_year",
        "listener_interests",
        "listener_signup_date_quintile",
        "listener_birth_year_vs_is_skipped_track_id_avg",
        "played_at_quarter_cume_dist",
        "second_listener_signup_date",
        "next_listener_signup_date",
        F.nth_value("played_at_quarter_seq", 2).over(w_all_by_played_at_full).alias("second_played_at_quarter_seq"),
        F.sum("listener_birth_year").over(w_source_listener_signup_date_2).alias("listener_birth_year_sum_2_prior"),
        F.datediff(F.col("listener_signup_date"), F.lag("listener_signup_date").over(w_source_listener_signup_date_3)).alias("days_since_prev_listener_signup_date"),
        F.round(F.avg("listener_birth_year").over(w_all_by_played_at_moving_2), 2).alias("listener_birth_year_avg_3"),
    )
)

result = (
    with_windows
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "played_at_quarter",
        "listener_birth_year",
        "listener_interests",
        "listener_signup_date_quintile",
        "listener_birth_year_vs_is_skipped_track_id_avg",
        "played_at_quarter_cume_dist",
        "second_listener_signup_date",
        "next_listener_signup_date",
        "second_played_at_quarter_seq",
        "listener_birth_year_sum_2_prior",
        "days_since_prev_listener_signup_date",
        "listener_birth_year_avg_3",
        F.length(F.trim(F.col("device_source_label"))).alias("device_source_label_len"),
        F.datediff(F.lit("2025-12-31").cast("date"), F.col("played_at")).alias("days_since_played_at"),
    )
)

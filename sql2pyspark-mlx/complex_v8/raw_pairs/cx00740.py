from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_listener_id_is_skipped_flag = Window.partitionBy("listener_id", "is_skipped_flag").orderBy(F.col("played_at_quarter").desc())
w_source_is_skipped_flag_by_pl = Window.partitionBy("source", "is_skipped_flag").orderBy("played_at_quarter", "id")
w_is_skipped_played_at_year = Window.partitionBy("is_skipped", F.year("played_at"))
w_source_is_skipped_by_is_skip = Window.partitionBy("source", "is_skipped").orderBy("is_skipped_flag", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_listener_id_is_skipped_flag_2 = Window.partitionBy("listener_id", "is_skipped_flag").orderBy(F.col("played_at_quarter").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_n_rows = Window.partitionBy("n_rows")

scoped_plays = (
    df_plays
    .filter((F.col("source").isin("album", "radio", "search")) & (F.col("played_at") < F.lit("2025-03-02 10:00:00").cast("timestamp")))
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "is_skipped",
        "source",
        F.date_trunc("quarter", F.col("played_at")).alias("played_at_quarter"),
        F.lower(F.trim(F.col("device"))).alias("device_clean"),
        F.concat_ws(" / ", F.col("device"), F.col("source")).alias("device_source_label"),
    )
)

scored = (
    scoped_plays
    .select(
        "id",
        "listener_id",
        "played_at",
        "is_skipped",
        "source",
        "played_at_quarter",
        "device_clean",
        "device_source_label",
        F.when(F.col("is_skipped"), 1).otherwise(0).alias("is_skipped_flag"),
        F.upper(F.trim(F.col("device_source_label"))).alias("device_source_label_clean"),
        F.concat(F.col("source"), F.lit("#"), F.col("track_id").cast("string")).alias("source_ref"),
    )
)

filtered = (
    scored.alias("s")
    .join(df_plays.alias("p"), (F.col("p.listener_id") == F.col("s.listener_id")) & (F.hour(F.col("p.played_at")).between(9, 14)), "left_anti")
)

windowed = (
    filtered
    .select(
        "id",
        "listener_id",
        "played_at",
        "is_skipped",
        "played_at_quarter",
        "device_clean",
        "device_source_label",
        "is_skipped_flag",
        "device_source_label_clean",
        "source_ref",
        F.round(F.cume_dist().over(w_listener_id_is_skipped_flag), 3).alias("played_at_quarter_cume_dist"),
        F.row_number().over(w_source_is_skipped_flag_by_pl).alias("played_at_quarter_seq"),
        F.round(F.col("is_skipped_flag") / F.nullif(F.sum("is_skipped_flag").over(w_is_skipped_played_at_year), F.lit(0)), 4).alias("is_skipped_flag_share_of_is_skipped_played_at_ye"),
        F.nth_value("is_skipped_flag", 2).over(w_source_is_skipped_by_is_skip).alias("second_is_skipped_flag"),
        F.last("played_at_quarter", ignorenulls=True).over(w_listener_id_is_skipped_flag_2).alias("last_known_played_at_quarter"),
        F.count(F.lit(1)).over(w_is_skipped_played_at_year).alias("rows_in_is_skipped_played_at_year"),
    )
)

joined_listener = (
    windowed.alias("s")
    .join(df_listeners.alias("l"), F.col("l.id") == F.col("s.listener_id"), "inner")
    .select(
        "s.id",
        "s.listener_id",
        "s.played_at",
        "s.device_source_label",
        "s.is_skipped_flag",
        "s.device_source_label_clean",
        "s.source_ref",
        "s.played_at_quarter_cume_dist",
        "s.played_at_quarter_seq",
        "s.is_skipped_flag_share_of_is_skipped_played_at_ye",
        "s.last_known_played_at_quarter",
        "s.rows_in_is_skipped_played_at_year",
        F.col("l.signup_date").alias("listener_signup_date"),
    )
)

rollup_base = (
    joined_listener
    .filter(F.year(F.col("listener_signup_date")) == 2024)
    .groupBy(
        F.year("last_known_played_at_quarter").alias("last_known_played_at_quarter_year"),
        "played_at_quarter_seq",
        F.date_trunc("month", "played_at").alias("played_at_month"),
    )
    .agg(
        F.count("*").alias("n_rows"),
        F.round(F.sum("played_at_quarter_seq") / F.nullif(F.sum("rows_in_is_skipped_played_at_year"), F.lit(0)), 3).alias("played_at_quarter_seq_to_rows_in_is_skipped_play"),
        F.min("played_at_quarter_cume_dist").alias("min_played_at_quarter_cume_dist"),
        F.countDistinct("listener_id").alias("distinct_listener_ids"),
        F.countDistinct("id").alias("distinct_ids"),
    )
)

with_trends = (
    rollup_base
    .select(
        "last_known_played_at_quarter_year",
        "played_at_quarter_seq",
        "played_at_month",
        "played_at_quarter_seq_to_rows_in_is_skipped_play",
        "min_played_at_quarter_cume_dist",
        "distinct_listener_ids",
        F.sum("distinct_listener_ids").over(w_n_rows).alias("n_rows_sum_distinct_listener_ids"),
        F.min("played_at_quarter_seq").over(w_n_rows).alias("n_rows_min_played_at_quarter_seq"),
    )
)

result = (
    with_trends
    .filter(F.col("distinct_listener_ids") >= 4)
    .select(
        "last_known_played_at_quarter_year",
        "played_at_quarter_seq",
        "played_at_quarter_seq_to_rows_in_is_skipped_play",
        "min_played_at_quarter_cume_dist",
        "distinct_listener_ids",
        "n_rows_min_played_at_quarter_seq",
    )
    .orderBy(
        "last_known_played_at_quarter_year",
        F.col("min_played_at_quarter_cume_dist").desc(),
    )
)

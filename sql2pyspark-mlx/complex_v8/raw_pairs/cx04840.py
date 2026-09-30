from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_listener_country_by_played_a = Window.partitionBy("listener_country").orderBy(F.col("played_at").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_is_skipped_track_id_by_playe = Window.partitionBy("is_skipped", "track_id").orderBy(F.col("played_at").desc())
w_source_by_played_at_running = Window.partitionBy("source").orderBy("played_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_listener_country_by_played_a_2 = Window.partitionBy("listener_country").orderBy(F.col("played_at").desc(), "id").rowsBetween(-2, -1)
w_listener_country = Window.partitionBy("listener_country")
w_source_by_played_at = Window.partitionBy("source").orderBy("played_at", "id")
w_listener_country_by_played_a_3 = Window.partitionBy("listener_country").orderBy(F.col("played_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

valid_plays = df_plays.filter(F.col("listener_id").isNotNull())

combined_details = (
    valid_plays.alias("s")
    .join(df_listeners.alias("l"), F.col("l.id") == F.col("s.listener_id"), "inner")
    .select("s.*", F.col("l.country").alias("listener_country"))
)

ranked = (
    combined_details
    .select(
        "*",
        F.nth_value("ms_played", 2).over(w_listener_country_by_played_a).alias("second_ms_played"),
        F.round(F.percent_rank().over(w_is_skipped_track_id_by_playe), 3).alias("played_at_percent_rank"),
        F.last("ms_played", ignorenulls=True).over(w_source_by_played_at_running).alias("last_known_ms_played"),
        F.round(F.avg("ms_played").over(w_listener_country_by_played_a_2), 2).alias("ms_played_avg_2_prior"),
        F.count(F.lit(1)).over(w_listener_country).alias("rows_in_listener_country"),
        F.row_number().over(w_source_by_played_at).alias("played_at_seq"),
        F.first("played_at").over(w_listener_country_by_played_a).alias("first_played_at"),
        F.nth_value("played_at", 2).over(w_listener_country_by_played_a).alias("second_played_at"),
        F.sum("ms_played").over(w_listener_country_by_played_a_3).alias("running_ms_played"),
        F.sum("ms_played").over(w_listener_country).alias("listener_country_sum_ms_played"),
    )
)

result = (
    ranked
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "device",
        "is_skipped",
        "source",
        "second_ms_played",
        "played_at_percent_rank",
        "ms_played_avg_2_prior",
        "rows_in_listener_country",
        "played_at_seq",
        "second_played_at",
        "running_ms_played",
        "listener_country_sum_ms_played",
    )
    .orderBy(F.col("source").desc(), F.col("played_at_seq").desc_nulls_first())
)

from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_played_at_month_is_skipped = Window.partitionBy(F.date_trunc("month", "played_at"), "is_skipped")
w_track_id_by_played_at_year_f = Window.partitionBy("track_id").orderBy(F.col("played_at_year").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_played_at_running = Window.orderBy("played_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

recent_plays = (
    df_plays
    .filter((F.col("source") == "radio") & (F.col("track_id").isNotNull()))
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "ms_played",
        "device",
        "is_skipped",
        "source",
        F.date_format(F.col("played_at"), "yyyy-MM").alias("played_at_label"),
        F.coalesce(F.col("ms_played"), F.lit(0)).alias("ms_played_filled"),
        F.date_trunc("year", F.col("played_at")).alias("played_at_year"),
    )
)

ranked = (
    recent_plays
    .select(
        "*",
        F.max("ms_played").over(w_played_at_month_is_skipped).alias("played_at_month_is_skipped_max_ms_played"),
        F.round(F.col("ms_played_filled") - F.avg("ms_played_filled").over(w_played_at_month_is_skipped), 2).alias("ms_played_filled_vs_played_at_month_is_skipped_a"),
        F.nth_value("ms_played", 2).over(w_track_id_by_played_at_year_f).alias("second_ms_played"),
        F.last("ms_played_filled", ignorenulls=True).over(w_all_by_played_at_running).alias("last_known_ms_played_filled"),
    )
)

exploded_detail = (
    ranked.alias("s")
    .join(df_plays.alias("p"), F.col("p.track_id") == F.col("s.track_id"), "left")
    .select(
        "s.id",
        "s.listener_id",
        "s.track_id",
        "s.played_at",
        "s.ms_played",
        "s.device",
        "s.is_skipped",
        "s.source",
        "s.played_at_label",
        "s.played_at_month_is_skipped_max_ms_played",
        "s.ms_played_filled_vs_played_at_month_is_skipped_a",
        "s.second_ms_played",
        F.col("p.id").alias("play_id"),
        F.col("p.is_skipped").alias("play_is_skipped"),
        F.col("p.ms_played").alias("play_ms_played"),
    )
)

result = (
    exploded_detail
    .select(
        "id",
        "play_id",
        "played_at_month_is_skipped_max_ms_played",
        "play_ms_played",
    )
    .unpivot(
        ids=["id", "play_id"],
        values=["played_at_month_is_skipped_max_ms_played", "play_ms_played"],
        variableColumnName="metric",
        valueColumnName="value",
    )
    .filter(F.col("value").isNotNull())
    .orderBy("id", "play_id", "metric")
)

from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_dedup = Window.partitionBy("device", "listener_id").orderBy("played_at", "id")
w_track_id_by_days_since_playe = Window.partitionBy("track_id").orderBy("days_since_played_at", "id")
w_listener_id_by_ms_played = Window.partitionBy("listener_id").orderBy("ms_played", "id")
w_track_id_by_days_since_playe_2 = Window.partitionBy("track_id").orderBy("days_since_played_at", "id").rowsBetween(-1, -1)
w_listener_plan = Window.partitionBy("listener_plan")

recent = (
    df_plays
    .filter(F.col("played_at") < F.lit("2025-02-18 10:00:00").cast("timestamp"))
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "ms_played",
        "device",
        "is_skipped",
        F.to_date(F.col("played_at")).alias("played_at_date"),
        F.datediff(F.lit("2026-01-01").cast("date"), F.col("played_at")).alias("days_since_played_at"),
    )
)

with_parent_listener = (
    recent.alias("s")
    .join(df_listeners.alias("l"), F.col("l.id") == F.col("s.listener_id"), "left")
    .select(
        "s.*",
        F.col("l.country").alias("listener_country"),
        F.col("l.username").alias("listener_username"),
        F.col("l.plan").alias("listener_plan"),
    )
)

latest_per_key = (
    with_parent_listener
    .withColumn("row_num", F.row_number().over(w_dedup))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

metrics = (
    latest_per_key
    .select(
        "*",
        F.ntile(4).over(w_track_id_by_days_since_playe).alias("days_since_played_at_quartile"),
        F.lead("played_at_date", 3).over(w_track_id_by_days_since_playe).alias("next3_played_at_date"),
        F.row_number().over(w_listener_id_by_ms_played).alias("ms_played_seq"),
        F.round(F.avg("days_since_played_at").over(w_track_id_by_days_since_playe_2), 2).alias("days_since_played_at_avg_1_prior"),
        F.count(F.lit(1)).over(w_listener_plan).alias("rows_in_listener_plan"),
    )
)

result = (
    metrics
    .filter(F.col("ms_played") >= 168782)
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "device",
        "played_at_date",
        "days_since_played_at",
        "listener_country",
        "listener_username",
        "listener_plan",
        "days_since_played_at_quartile",
        "next3_played_at_date",
        "ms_played_seq",
        "rows_in_listener_plan",
        F.concat_ws(" / ", F.col("listener_plan"), F.col("listener_username")).alias("listener_plan_listener_username_label"),
    )
    .orderBy(F.col("next3_played_at_date").asc_nulls_last())
)

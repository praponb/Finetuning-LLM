from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_plan = Window.partitionBy("plan")

cleaned_listeners = (
    df_listeners
    .filter(~F.col("country").isin("BR", "UK"))
    .select(
        "id",
        "username",
        "plan",
        "signup_date",
    )
)

filtered = (
    cleaned_listeners.alias("s")
    .join(df_plays.alias("p"), F.col("p.listener_id") == F.col("s.id"), "left_semi")
    .filter(F.col("signup_date") >= F.lit("2024-07-13").cast("date"))
)

play_stats = (
    df_plays
    .groupBy("listener_id")
    .agg(
        F.max("played_at").alias("last_played_at"),
        F.round(F.avg("ms_played"), 2).alias("avg_play_ms_played"),
    )
)

with_stats = (
    filtered.alias("s")
    .join(play_stats.alias("p"), F.col("p.listener_id") == F.col("s.id"), "left")
    .select("s.*", "p.last_played_at", "p.avg_play_ms_played")
)

with_windows = (
    with_stats
    .select(
        "id",
        "plan",
        "signup_date",
        "last_played_at",
        "avg_play_ms_played",
        F.round(F.avg("avg_play_ms_played").over(w_plan), 2).alias("plan_avg_avg_play_ms_played"),
        F.sum("avg_play_ms_played").over(w_plan).alias("plan_sum_avg_play_ms_played"),
    )
)

result = (
    with_windows
    .filter(F.col("plan_sum_avg_play_ms_played").between(1203500.0, 3001250.0))
    .select(
        "id",
        "plan",
        "signup_date",
        "avg_play_ms_played",
        "plan_avg_avg_play_ms_played",
        "plan_sum_avg_play_ms_played",
        F.coalesce(F.col("plan"), F.lit("UNKNOWN")).alias("plan_or_unknown"),
        F.concat(F.col("plan"), F.lit("#"), F.col("id").cast("string")).alias("plan_ref"),
    )
    .orderBy("plan", F.col("plan_or_unknown").desc_nulls_first())
)

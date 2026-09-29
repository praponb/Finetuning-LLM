from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_listener_id_by_prior_rows_3d = Window.partitionBy("listener_id").orderBy(F.col("prior_rows_3d").desc(), "id")
w_listener_id_by_prior_rows_3d_2 = Window.partitionBy("listener_id").orderBy(F.col("prior_rows_3d").desc())
w_prior_rows_3d_listener_id_by = Window.partitionBy("prior_rows_3d", "listener_id").orderBy("prior_rows_3d", "id")

joined_ranges = (
    df_plays.alias("a")
    .join(
        df_plays.alias("b"),
        (F.col("b.listener_id") == F.col("a.listener_id"))
        & (F.col("b.played_at") < F.col("a.played_at"))
        & (F.col("b.played_at") >= F.col("a.played_at") - F.expr("INTERVAL 3 DAYS")),
        "left",
    )
    .groupBy("a.id", "a.listener_id", "a.played_at")
    .agg(
        F.count("b.id").alias("prior_rows_3d"),
        F.sum("b.ms_played").alias("prior_ms_played_3d"),
    )
)

sequenced = (
    joined_ranges
    .select(
        "*",
        F.round((F.unix_timestamp("played_at") - F.unix_timestamp(F.lag("played_at").over(w_listener_id_by_prior_rows_3d))) / 60, 1).alias("minutes_since_prev_played_at"),
        F.round(F.percent_rank().over(w_listener_id_by_prior_rows_3d_2), 3).alias("prior_rows_3d_percent_rank"),
        F.lead("prior_ms_played_3d").over(w_prior_rows_3d_listener_id_by).alias("next_prior_ms_played_3d"),
    )
)

enriched = (
    sequenced
    .filter(F.col("minutes_since_prev_played_at") > 1500.0)
    .withColumn("played_at_hour", F.hour(F.col("played_at")))
)

listener_id_stats = (
    enriched
    .groupBy("listener_id")
    .agg(F.max("minutes_since_prev_played_at").alias("max_minutes_since_prev_played_at"))
)

above_average = (
    enriched.alias("s")
    .join(listener_id_stats.alias("g"), F.col("g.listener_id") == F.col("s.listener_id"), "inner")
    .filter(F.col("s.minutes_since_prev_played_at") == F.col("g.max_minutes_since_prev_played_at"))
    .select(
        "s.*",
        "g.max_minutes_since_prev_played_at",
        F.round(F.col("s.minutes_since_prev_played_at") - F.col("g.max_minutes_since_prev_played_at"), 2).alias("minutes_since_prev_played_at_minus_max"),
    )
)

result = (
    above_average
    .select(
        "id",
        "played_at",
        "prior_rows_3d",
        "prior_ms_played_3d",
        "minutes_since_prev_played_at",
        "max_minutes_since_prev_played_at",
        "minutes_since_prev_played_at_minus_max",
        (F.col("played_at_hour") + F.col("prior_ms_played_3d")).alias("played_at_hour_plus_prior_ms_played_3d"),
    )
    .orderBy("played_at")
)

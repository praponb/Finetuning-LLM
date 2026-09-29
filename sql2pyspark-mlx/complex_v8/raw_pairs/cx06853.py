from pyspark.sql import functions as F

in_window = (
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

joined_listener = (
    in_window.alias("s")
    .join(df_listeners.alias("l"), F.col("l.id") == F.col("s.listener_id"), "left")
    .select(
        "s.id",
        "s.listener_id",
        "s.played_at",
        "s.prior_rows_3d",
        "s.prior_ms_played_3d",
        F.col("l.country").alias("listener_country"),
        F.col("l.username").alias("listener_username"),
    )
)

listener_country_stats = (
    joined_listener
    .groupBy("listener_country")
    .agg(F.avg("prior_rows_3d").alias("avg_prior_rows_3d"))
)

above_average = (
    joined_listener.alias("s")
    .join(listener_country_stats.alias("g"), F.col("g.listener_country") == F.col("s.listener_country"), "inner")
    .filter(F.col("s.prior_rows_3d") > F.col("g.avg_prior_rows_3d"))
    .select(
        "s.*",
        "g.avg_prior_rows_3d",
        F.round(F.col("s.prior_rows_3d") - F.col("g.avg_prior_rows_3d"), 2).alias("prior_rows_3d_minus_avg"),
    )
)

result = (
    above_average
    .groupBy("listener_id")
    .pivot("listener_country", ["BR", "UK", "TH"])
    .agg(F.sum("prior_rows_3d"))
)

from pyspark.sql import functions as F

joined_ranges = (
    df_plays.alias("a")
    .join(
        df_plays.alias("b"),
        (F.col("b.listener_id") == F.col("a.listener_id"))
        & (F.col("b.played_at") < F.col("a.played_at"))
        & (F.col("b.played_at") >= F.col("a.played_at") - F.expr("INTERVAL 12 HOURS")),
        "left",
    )
    .groupBy("a.id", "a.listener_id", "a.played_at")
    .agg(
        F.count("b.id").alias("prior_rows_12h"),
        F.sum("b.ms_played").alias("prior_ms_played_12h"),
    )
)

derived = joined_ranges.select("*", F.hour(F.col("played_at")).alias("played_at_hour"))

derived_2 = (
    derived
    .filter(F.to_date(F.col("played_at")) < F.lit("2025-03-02").cast("date"))
    .select(
        "*",
        F.coalesce(F.col("prior_rows_12h"), F.lit(0)).alias("prior_rows_12h_filled"),
        F.abs(F.col("played_at_hour") - 11).alias("played_at_hour_distance"),
        F.dayofweek(F.col("played_at")).alias("played_at_dayofweek"),
    )
)

exploded_detail = (
    derived_2.alias("s")
    .join(df_plays.alias("p"), F.col("p.listener_id") == F.col("s.listener_id"), "inner")
    .select(
        "s.id",
        "s.listener_id",
        "s.played_at",
        "s.prior_rows_12h",
        "s.prior_ms_played_12h",
        "s.played_at_hour",
        "s.prior_rows_12h_filled",
        "s.played_at_dayofweek",
        F.col("p.id").alias("play_id"),
        F.col("p.device").alias("play_device"),
        F.col("p.played_at").alias("play_played_at"),
        F.col("p.ms_played").alias("play_ms_played"),
    )
)

result = (
    exploded_detail
    .groupBy("played_at_hour", "play_id")
    .pivot("play_device", ["speaker", "phone", "desktop", "car"])
    .agg(F.min("prior_ms_played_12h"))
)

from pyspark.sql import functions as F

matched = (
    df_plays.alias("a")
    .join(
        df_plays.alias("b"),
        (F.col("b.listener_id") == F.col("a.listener_id"))
        & (F.col("b.played_at") < F.col("a.played_at"))
        & (F.col("b.played_at") >= F.col("a.played_at") - F.expr("INTERVAL 7 DAYS")),
        "left",
    )
    .groupBy("a.id", "a.listener_id", "a.played_at")
    .agg(
        F.count("b.id").alias("prior_rows_7d"),
        F.sum("b.ms_played").alias("prior_ms_played_7d"),
    )
)

summary = (
    matched
    .filter(F.col("prior_rows_7d") > 1)
    .groupBy("prior_rows_7d", "listener_id")
    .agg(
        F.count("*").alias("row_count"),
        F.max("prior_ms_played_7d").alias("max_prior_ms_played_7d"),
        F.countDistinct("id").alias("distinct_ids"),
        F.min("played_at").alias("first_played_at"),
        F.max("played_at").alias("last_played_at"),
    )
)

with_children_plays = (
    summary.alias("s")
    .join(df_plays.alias("p"), (F.col("p.listener_id") == F.col("s.listener_id")) & (F.col("p.device").isin("desktop", "car")), "inner")
    .select(
        "s.prior_rows_7d",
        "s.listener_id",
        "s.last_played_at",
        F.col("p.id").alias("play_id"),
        F.col("p.played_at").alias("play_played_at"),
        F.col("p.device").alias("play_device"),
    )
)

enriched = (
    with_children_plays
    .select(
        "*",
        F.lower(F.regexp_replace(F.trim(F.col("play_device")), " ", "_")).alias("play_device_slug"),
        F.to_date(F.col("last_played_at")).alias("last_played_at_date"),
        F.date_trunc("year", F.col("play_played_at")).alias("play_played_at_year"),
    )
)

result = (
    enriched
    .select(
        "prior_rows_7d",
        "listener_id",
        "last_played_at",
        "play_played_at",
        "play_device",
        "last_played_at_date",
        "play_played_at_year",
        F.date_trunc("week", F.col("last_played_at")).alias("last_played_at_week"),
        F.lower(F.regexp_replace(F.trim(F.col("play_device_slug")), " ", "_")).alias("play_device_slug_slug"),
    )
    .orderBy(
        F.col("play_played_at_year").desc_nulls_first(),
        F.col("play_device").desc(),
    )
)

from pyspark.sql import functions as F

recent = (
    df_plays
    .filter(~F.col("is_skipped"))
    .select(
        "id",
        "listener_id",
        "track_id",
        "ms_played",
        "device",
        "is_skipped",
        F.lower(F.regexp_replace(F.trim(F.col("device")), " ", "_")).alias("device_slug"),
        F.date_trunc("year", F.col("played_at")).alias("played_at_year"),
    )
)

screened = (
    recent.alias("s")
    .join(df_plays.alias("p"), (F.col("p.track_id") == F.col("s.track_id")) & (F.col("p.is_skipped")), "left_anti")
)

result = (
    screened
    .filter(F.col("device").isin("speaker", "desktop", "car"))
    .select(
        "id",
        "track_id",
        "ms_played",
        "device",
        "device_slug",
        "played_at_year",
        F.when(F.col("is_skipped"), "yes").otherwise("no").alias("is_skipped_label"),
        (F.col("played_at_year") + F.expr("INTERVAL 2 HOURS")).alias("played_at_year_plus_2h"),
    )
    .orderBy(F.col("ms_played").desc())
)

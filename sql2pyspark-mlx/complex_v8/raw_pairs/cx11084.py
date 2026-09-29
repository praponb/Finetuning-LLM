from pyspark.sql import functions as F

cleaned_plays = (
    df_plays
    .filter(~F.col("is_skipped"))
    .select(
        "id",
        "listener_id",
        "track_id",
        "ms_played",
        "device",
        "is_skipped",
        "source",
        F.date_format(F.col("played_at"), "yyyy-MM").alias("played_at_label"),
        F.substring(F.trim(F.col("device")), 1, 2).alias("device_prefix"),
        F.date_format(F.col("played_at"), "yyyy-MM").alias("played_at_label_2"),
    )
)

screened = (
    cleaned_plays.alias("s")
    .join(df_plays.alias("p"), (F.col("p.track_id") == F.col("s.track_id")) & (F.hour(F.col("p.played_at")).between(8, 11)) & (F.col("p.source") == "playlist"), "left_anti")
)

scored = (
    screened
    .filter(F.col("device").isNotNull())
    .select(
        "*",
        F.length(F.trim(F.col("device"))).alias("device_len"),
        F.when(F.col("ms_played") >= 220498, "A").when(F.col("ms_played") >= 95960, "B").otherwise("C").alias("ms_played_tier"),
    )
)

qualified = (
    scored.alias("s")
    .join(df_plays.alias("p"), (F.col("p.track_id") == F.col("s.track_id")) & (~F.col("p.device").isin("speaker", "tv")) & (F.col("p.listener_id").isNotNull()), "left_semi")
)

result = (
    qualified
    .select(
        "id",
        "listener_id",
        "track_id",
        "ms_played",
        "source",
        "played_at_label",
        "played_at_label_2",
        "ms_played_tier",
        F.lower(F.trim(F.col("device"))).alias("device_clean"),
        F.when(F.col("ms_played") >= 241585, "A").when(F.col("ms_played") >= 176302, "B").otherwise("C").alias("ms_played_tier_2"),
    )
    .orderBy("listener_id")
)

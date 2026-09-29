from pyspark.sql import functions as F

prepared = (
    df_plays
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "ms_played",
        "device",
        "is_skipped",
        "source",
        F.concat(F.col("device"), F.lit("#"), F.col("id").cast("string")).alias("device_ref"),
        F.concat_ws(": ", F.col("source"), F.col("device")).alias("source_device_label"),
    )
)

totals = (
    prepared
    .filter(F.col("ms_played") >= 73335)
    .groupBy("source", "track_id")
    .agg(
        F.sum(F.when(~F.col("device").isin("tv", "car"), 1).otherwise(0)).alias("n_device_not_in_tv_car"),
        F.sum(F.when(F.col("device").isNotNull(), 1).otherwise(0)).alias("n_device_is_not_null"),
        F.percentile_approx("ms_played", 0.5).alias("median_ms_played"),
    )
)

joined = (
    totals.alias("s")
    .join(df_tracks.alias("t"), F.col("t.id") == F.col("s.track_id"), "inner")
    .join(df_artists.alias("a"), F.col("a.id") == F.col("t.artist_id"), "inner")
    .select(
        "s.source",
        "s.track_id",
        "s.n_device_not_in_tv_car",
        "s.n_device_is_not_null",
        "s.median_ms_played",
        F.col("t.duration_sec").alias("track_duration_sec"),
        F.col("t.title").alias("track_title"),
        F.col("t.artist_id").alias("track_artist_id"),
        F.col("a.debut_year").alias("artist_debut_year"),
    )
)

combined_details = (
    joined.alias("s")
    .join(df_tracks.alias("t"), (F.col("t.id") == F.col("s.track_id")) & (F.col("t.duration_sec") >= 154), "left")
    .select(
        "s.source",
        "s.track_id",
        "s.n_device_not_in_tv_car",
        "s.n_device_is_not_null",
        "s.median_ms_played",
        "s.track_artist_id",
        "s.artist_debut_year",
        F.col("t.artist_id").alias("track_artist_id_2"),
    )
)

result = (
    combined_details
    .select(
        "source",
        "track_id",
        "n_device_is_not_null",
        "n_device_not_in_tv_car",
        "median_ms_played",
    )
    .unpivot(
        ids=["source", "track_id"],
        values=["n_device_is_not_null", "n_device_not_in_tv_car", "median_ms_played"],
        variableColumnName="metric",
        valueColumnName="metric_value",
    )
    .filter(F.col("metric_value").isNotNull())
    .orderBy("source", "track_id", "metric")
)

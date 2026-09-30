from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_source_by_ms_played_distance = Window.partitionBy("source").orderBy(F.col("ms_played_distance").desc(), "id")
w_device = Window.partitionBy("device")

cleaned = (
    df_plays
    .filter(F.col("track_id").isNotNull())
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "ms_played",
        "device",
        "is_skipped",
        "source",
        F.date_format(F.col("played_at"), "MMM yyyy").alias("played_at_label"),
    )
)

derived = (
    cleaned
    .filter(F.col("device").isin("car", "speaker"))
    .select(
        "id",
        "ms_played",
        "device",
        "is_skipped",
        "source",
        "played_at_label",
        F.abs(F.col("ms_played") - 112786).alias("ms_played_distance"),
    )
)

metrics = (
    derived
    .select(
        "id",
        "device",
        "is_skipped",
        "source",
        "played_at_label",
        F.round(F.cume_dist().over(w_source_by_ms_played_distance), 3).alias("ms_played_distance_cume_dist"),
        F.count(F.lit(1)).over(w_device).alias("rows_in_device"),
        F.round(F.col("ms_played") - F.avg("ms_played").over(w_device), 2).alias("ms_played_vs_device_avg"),
        (F.col("ms_played_distance") - F.lag("ms_played_distance").over(w_source_by_ms_played_distance)).alias("ms_played_distance_change"),
    )
)

per_group = (
    metrics
    .filter(F.col("ms_played_vs_device_avg") >= -320000.0)
    .groupBy("device", "source", "is_skipped")
    .agg(
        F.min("rows_in_device").alias("min_rows_in_device"),
        F.countDistinct("device").alias("distinct_devices"),
        F.round(F.sum("rows_in_device") / F.nullif(F.sum("ms_played_distance_change"), F.lit(0)), 3).alias("rows_in_device_to_ms_played_distance_change_rati"),
        F.count("*").alias("cnt"),
        F.count(F.when(F.col("source") == "playlist", 1)).alias("n_source_playlist"),
        F.sum("rows_in_device").alias("total_rows_in_device"),
    )
    .filter(F.col("cnt") >= 3)
)

annotated = (
    per_group
    .filter(F.col("distinct_devices") > 4)
    .withColumn("source_ref", F.concat(F.col("source"), F.lit("#"), F.col("min_rows_in_device").cast("string")))
)

scored = (
    annotated
    .select(
        "device",
        "source",
        "is_skipped",
        "distinct_devices",
        "cnt",
        "n_source_playlist",
        "total_rows_in_device",
        "source_ref",
        (F.col("min_rows_in_device") + F.col("cnt")).alias("min_rows_in_device_plus_cnt"),
        F.when(F.col("source").isin("album", "playlist"), "group_a").when(F.col("source") == "search", "group_b").otherwise("other").alias("source_group"),
        F.coalesce(F.col("source"), F.lit("UNKNOWN")).alias("source_or_unknown"),
    )
)

source_group_stats = (
    scored
    .groupBy("source_group")
    .agg(F.percentile_approx("distinct_devices", 0.5).alias("median_distinct_devices"))
)

above_average = (
    scored.alias("s")
    .join(source_group_stats.alias("g"), F.col("g.source_group") == F.col("s.source_group"), "inner")
    .filter(F.col("s.distinct_devices") >= F.col("g.median_distinct_devices"))
    .select(
        "s.*",
        "g.median_distinct_devices",
        F.round(F.col("s.distinct_devices") - F.col("g.median_distinct_devices"), 2).alias("distinct_devices_minus_median"),
    )
)

result = (
    above_average
    .select(
        "device",
        "is_skipped",
        "distinct_devices",
        "cnt",
        "n_source_playlist",
        "total_rows_in_device",
        "source_ref",
        "min_rows_in_device_plus_cnt",
        "source_group",
        "median_distinct_devices",
        F.greatest(F.col("total_rows_in_device"), F.col("n_source_playlist")).alias("greatest_total_rows_in_device_n_source_playlist"),
        F.coalesce(F.col("n_source_playlist"), F.lit(0)).alias("n_source_playlist_filled"),
    )
    .orderBy(F.col("source_group").asc_nulls_last())
)

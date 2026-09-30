from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_latest = Window.partitionBy("listener_id").orderBy(F.col("ms_played_distance").desc(), F.col("id").desc())
w_track_id_cnt = Window.partitionBy("track_id", "cnt")
w_cnt_track_id_by_ms_played_di = Window.partitionBy("cnt", "track_id").orderBy(F.col("ms_played_distance_to_ms_played_bucket_ratio").desc())

prepared_plays = (
    df_plays
    .filter((F.col("device") == "phone") | (F.col("ms_played") >= 44439))
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "ms_played",
        "device",
        "is_skipped",
        F.concat_ws(" / ", F.col("device"), F.col("source")).alias("device_source_label"),
        F.abs(F.col("ms_played") - 178909).alias("ms_played_distance"),
        (F.floor(F.col("ms_played") / 50000) * 50000).alias("ms_played_bucket"),
    )
)

latest = (
    prepared_plays
    .withColumn("row_num", F.row_number().over(w_latest))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

per_group = (
    latest
    .groupBy("track_id")
    .agg(
        F.round(F.sum("ms_played_distance") / F.nullif(F.sum("ms_played_bucket"), F.lit(0)), 3).alias("ms_played_distance_to_ms_played_bucket_ratio"),
        F.count("*").alias("cnt"),
        F.round(F.stddev("ms_played_bucket"), 2).alias("stddev_ms_played_bucket"),
        F.round(F.sum("ms_played_distance") / F.nullif(F.sum("ms_played"), F.lit(0)), 3).alias("ms_played_distance_to_ms_played_ratio"),
        F.max("ms_played").alias("max_ms_played_check"),
    )
    .filter(F.col("max_ms_played_check") > 95960)
    .drop("max_ms_played_check")
)

ranked = (
    per_group
    .select(
        "track_id",
        "ms_played_distance_to_ms_played_bucket_ratio",
        "cnt",
        "stddev_ms_played_bucket",
        "ms_played_distance_to_ms_played_ratio",
        F.round(F.col("cnt") - F.avg("cnt").over(w_track_id_cnt), 2).alias("cnt_vs_track_id_cnt_avg"),
        F.rank().over(w_cnt_track_id_by_ms_played_di).alias("ms_played_distance_to_ms_played_bucket_ratio_ran"),
        F.round(F.col("cnt") / F.nullif(F.sum("cnt").over(w_track_id_cnt), F.lit(0)), 4).alias("cnt_share_of_track_id_cnt"),
    )
)

annotated = (
    ranked
    .filter(F.col("ms_played_distance_to_ms_played_bucket_ratio_ran") <= 9)
    .select(
        "track_id",
        "cnt",
        "stddev_ms_played_bucket",
        "ms_played_distance_to_ms_played_ratio",
        "cnt_vs_track_id_cnt_avg",
        "ms_played_distance_to_ms_played_bucket_ratio_ran",
        F.when(F.col("cnt") >= 9, "A").when(F.col("cnt") >= 5, "B").otherwise("C").alias("cnt_tier"),
        F.coalesce(F.col("cnt_vs_track_id_cnt_avg"), F.lit(0.0)).alias("cnt_vs_track_id_cnt_avg_filled"),
        (F.col("cnt_vs_track_id_cnt_avg") + F.col("cnt")).alias("cnt_vs_track_id_cnt_avg_plus_cnt"),
    )
)

qualified = (
    annotated.alias("s")
    .join(df_plays.alias("p"), F.col("p.track_id") == F.col("s.track_id"), "left_semi")
)

result = (
    qualified
    .select("track_id", "ms_played_distance_to_ms_played_bucket_ratio_ran", "cnt")
    .unpivot(
        ids=["track_id"],
        values=["ms_played_distance_to_ms_played_bucket_ratio_ran", "cnt"],
        variableColumnName="field",
        valueColumnName="metric_value",
    )
)

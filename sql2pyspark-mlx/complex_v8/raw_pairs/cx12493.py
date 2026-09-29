from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_device_by_ms_played = Window.partitionBy("device").orderBy(F.col("ms_played").desc(), "id")
w_source = Window.partitionBy("source")

base_plays = (
    df_plays
    .filter((F.col("device") == "phone") & (F.col("listener_id").isNotNull()))
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "ms_played",
        "device",
        "is_skipped",
        "source",
        F.coalesce(F.col("device"), F.lit("UNKNOWN")).alias("device_or_unknown"),
    )
)

sequenced = (
    base_plays
    .select(
        "id",
        "track_id",
        "ms_played",
        "device",
        "is_skipped",
        "device_or_unknown",
        F.dense_rank().over(w_device_by_ms_played).alias("ms_played_dense_rank"),
        F.max("ms_played").over(w_source).alias("source_max_ms_played"),
        (F.col("ms_played") - F.lag("ms_played").over(w_device_by_ms_played)).alias("ms_played_change"),
        F.size(F.collect_set("track_id").over(w_source)).alias("distinct_track_id_in_source"),
        F.count(F.lit(1)).over(w_source).alias("rows_in_source"),
        F.round(F.col("ms_played") / F.nullif(F.sum("ms_played").over(w_source), F.lit(0)), 4).alias("ms_played_share_of_source"),
    )
)

screened = (
    sequenced
    .crossJoin(sequenced.agg(F.avg("ms_played_change").alias("overall_avg_ms_played_change")))
    .filter(F.col("ms_played_change") > F.col("overall_avg_ms_played_change"))
    .drop("overall_avg_ms_played_change")
)

result = (
    screened
    .select(
        "id",
        "track_id",
        "ms_played",
        "device",
        "device_or_unknown",
        "source_max_ms_played",
        "ms_played_change",
        "distinct_track_id_in_source",
        "rows_in_source",
        "ms_played_share_of_source",
    )
    .orderBy(
        F.col("device_or_unknown").desc(),
        F.col("distinct_track_id_in_source").desc(),
    )
)

from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_source_by_played_at = Window.partitionBy("source").orderBy(F.col("played_at").desc(), "id")
w_all_by_played_at_ties = Window.orderBy("played_at")

scoped = (
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
        F.length(F.trim(F.col("device"))).alias("device_len"),
        F.concat(F.col("device"), F.lit("#"), F.col("ms_played").cast("string")).alias("device_ref"),
    )
)

screened = (
    scoped.alias("s")
    .join(df_plays.alias("p"), F.col("p.listener_id") == F.col("s.listener_id"), "left_semi")
)

metrics = (
    screened
    .select(
        "*",
        (F.col("device_len") - F.lag("device_len").over(w_source_by_played_at)).alias("device_len_change"),
        F.round(F.cume_dist().over(w_all_by_played_at_ties), 3).alias("played_at_cume_dist"),
    )
)

result = (
    metrics
    .groupBy("is_skipped", "device_len")
    .pivot("device", ["desktop", "car", "phone", "tv"])
    .agg(F.avg("device_len_change"))
)

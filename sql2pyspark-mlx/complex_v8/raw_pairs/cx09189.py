from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_is_skipped_by_ms_played_buck = Window.partitionBy("is_skipped").orderBy(F.col("ms_played_bucket").desc(), "id")
w_is_skipped = Window.partitionBy("is_skipped")

base = (
    df_plays
    .filter((F.col("listener_id").isNotNull()) & (F.col("device").isin("phone", "desktop")))
    .select(
        "id",
        "listener_id",
        "track_id",
        "ms_played",
        "device",
        "is_skipped",
    )
)

screened = (
    base.alias("s")
    .join(df_plays.alias("p"), (F.col("p.track_id") == F.col("s.track_id")) & (~F.col("p.is_skipped")), "left_anti")
)

annotated = (
    screened
    .select(
        "*",
        F.concat(F.col("device"), F.lit("#"), F.col("listener_id").cast("string")).alias("device_ref"),
        (F.floor(F.col("ms_played") / 50000) * 50000).alias("ms_played_bucket"),
        F.substring(F.trim(F.col("device")), 1, 4).alias("device_prefix"),
    )
)

metrics = (
    annotated
    .select(
        "id",
        "listener_id",
        "is_skipped",
        "device_ref",
        "ms_played_bucket",
        F.ntile(4).over(w_is_skipped_by_ms_played_buck).alias("ms_played_bucket_quartile"),
        F.round(F.col("ms_played") - F.avg("ms_played").over(w_is_skipped), 2).alias("ms_played_vs_is_skipped_avg"),
    )
)

result = (
    metrics
    .groupingSets(
        [["listener_id", "ms_played_bucket_quartile"], ["listener_id"], []],
        "listener_id", "ms_played_bucket_quartile",
    )
    .agg(F.sum("ms_played_bucket").alias("total_ms_played_bucket"))
)

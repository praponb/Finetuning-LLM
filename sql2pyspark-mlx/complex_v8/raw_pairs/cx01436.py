from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_latest = Window.partitionBy("device", "listener_id").orderBy(F.col("source_len_plus_ms_played").desc(), F.col("id").desc())

cleaned = (
    df_plays
    .filter((F.col("ms_played") > 56848) & (~F.col("source").isin("playlist", "radio")))
    .select(
        "id",
        "listener_id",
        "track_id",
        "ms_played",
        "device",
        "is_skipped",
        "source",
        F.concat(F.col("device"), F.lit("#"), F.col("id").cast("string")).alias("device_ref"),
        F.length(F.trim(F.col("source"))).alias("source_len"),
        F.lower(F.regexp_replace(F.trim(F.col("source")), " ", "_")).alias("source_slug"),
    )
)

annotated = (
    cleaned
    .select(
        "*",
        F.substring(F.trim(F.col("source_slug")), 1, 2).alias("source_slug_prefix"),
        (F.col("source_len") + F.col("ms_played")).alias("source_len_plus_ms_played"),
        F.coalesce(F.col("source_len"), F.lit(0)).alias("source_len_filled"),
    )
)

latest_per_key = (
    annotated
    .withColumn("rn", F.row_number().over(w_latest))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

with_details_listener = (
    latest_per_key.alias("s")
    .join(df_listeners.alias("l"), (F.col("l.id") == F.col("s.listener_id")) & (F.upper(F.trim(F.col("l.username"))).like("G%")), "inner")
    .select(
        "s.id",
        "s.listener_id",
        "s.track_id",
        "s.ms_played",
        "s.is_skipped",
        "s.source",
        "s.device_ref",
        "s.source_slug_prefix",
        "s.source_len_plus_ms_played",
        "s.source_len_filled",
        F.col("l.signup_date").alias("listener_signup_date"),
        F.col("l.username").alias("listener_username"),
    )
)

result = (
    with_details_listener
    .select("ms_played", "listener_id")
    .distinct()
    .orderBy(F.col("listener_id").desc())
)

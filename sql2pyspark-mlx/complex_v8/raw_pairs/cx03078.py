from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_dedup = Window.partitionBy("track_id").orderBy(F.col("ms_played_plus_ms_played_bucket").desc(), F.col("id").desc())

base_plays = (
    df_plays
    .filter(F.month(F.col("played_at")).isin(1, 2))
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "ms_played",
        "device",
        "is_skipped",
        "source",
        F.when(F.col("device").isin("desktop", "phone"), "group_a").when(F.col("device") == "tv", "group_b").otherwise("other").alias("device_group"),
        (F.floor(F.col("ms_played") / 50000) * 50000).alias("ms_played_bucket"),
    )
)

tagged = (
    base_plays
    .select(
        "*",
        (F.col("ms_played") + F.col("ms_played_bucket")).alias("ms_played_plus_ms_played_bucket"),
        F.substring(F.trim(F.col("device")), 1, 4).alias("device_prefix"),
        (F.col("played_at") + F.expr("INTERVAL 30 MINUTES")).alias("played_at_plus_30m"),
    )
)

latest_per_key = (
    tagged
    .withColumn("row_num", F.row_number().over(w_dedup))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

result = (
    latest_per_key
    .rollup("source", "track_id")
    .agg(
        F.min("played_at").alias("first_played_at"),
        F.count("*").alias("n_rows"),
    )
    .orderBy(F.col("source").asc_nulls_last(), F.col("track_id").asc_nulls_last())
)

from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_latest = Window.partitionBy("track_id").orderBy(F.col("ms_played").desc(), F.col("id").desc())

scoped_plays = (
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
    )
)

play_stats = (
    df_plays
    .groupBy("track_id")
    .agg(F.count("*").alias("play_count"))
)

profiled = (
    scoped_plays.alias("s")
    .join(play_stats.alias("p"), F.col("p.track_id") == F.col("s.track_id"), "left")
    .select("s.*", F.coalesce(F.col("p.play_count"), F.lit(0)).alias("play_count"))
)

latest_per_key = (
    profiled
    .withColumn("row_num", F.row_number().over(w_latest))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

result = (
    latest_per_key
    .select(
        "id",
        "listener_id",
        "played_at",
        "ms_played",
        "device",
        "is_skipped",
        "play_count",
        F.when(F.col("device").isin("desktop", "tv"), "group_a").when(F.col("device") == "phone", "group_b").otherwise("other").alias("device_group"),
    )
    .orderBy(F.col("device_group").desc())
)

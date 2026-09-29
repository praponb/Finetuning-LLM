from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_dedup = Window.partitionBy("device").orderBy(F.col("play_device_count").desc(), F.col("id").desc())
w_played_at_year_by_played_at = Window.partitionBy(F.year("played_at")).orderBy("played_at")
w_played_at_year_by_played_at_2 = Window.partitionBy(F.year("played_at")).orderBy("played_at", "id")
w_played_at_month_play_device = Window.partitionBy(F.month("played_at"), "play_device_count")
w_dedup_2 = Window.partitionBy("track_id").orderBy(F.col("next_played_at_date").desc(), F.col("id").desc())

filtered = (
    df_plays
    .filter(F.col("source") == "search")
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "ms_played",
        "device",
        "is_skipped",
        F.abs(F.col("ms_played") - 195566).alias("ms_played_distance"),
        F.concat(F.col("source"), F.lit("#"), F.col("id").cast("string")).alias("source_ref"),
        F.to_date(F.col("played_at")).alias("played_at_date"),
    )
)

play_stats = (
    df_plays
    .groupBy("track_id")
    .agg(
        F.count("*").alias("play_count"),
        F.count(F.when(F.col("device").isNotNull(), 1)).alias("play_device_count"),
    )
)

summarized_rows = (
    filtered.alias("s")
    .join(play_stats.alias("p"), F.col("p.track_id") == F.col("s.track_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("p.play_count"), F.lit(0)).alias("play_count"),
        F.coalesce(F.col("p.play_device_count"), F.lit(0)).alias("play_device_count"),
    )
)

latest = (
    summarized_rows
    .withColumn("recency_rank", F.row_number().over(w_dedup))
    .filter(F.col("recency_rank") == 1)
    .drop("recency_rank")
)

ranked = (
    latest
    .select(
        "*",
        F.round(F.cume_dist().over(w_played_at_year_by_played_at), 3).alias("played_at_cume_dist"),
        F.lead("played_at_date").over(w_played_at_year_by_played_at_2).alias("next_played_at_date"),
        F.round(F.col("play_count") / F.nullif(F.sum("play_count").over(w_played_at_month_play_device), F.lit(0)), 4).alias("play_count_share_of_played_at_month_play_device_"),
    )
)

unique_rows = (
    ranked
    .withColumn("row_num", F.row_number().over(w_dedup_2))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

scored = (
    unique_rows
    .filter(F.col("source_ref").isNotNull())
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "ms_played",
        "device",
        "is_skipped",
        "ms_played_distance",
        "source_ref",
        "played_at_date",
        "play_count",
        "play_device_count",
        "played_at_cume_dist",
        F.date_trunc("quarter", F.col("played_at_date")).alias("played_at_date_quarter"),
    )
)

result = (
    scored
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "ms_played",
        "is_skipped",
        "played_at_date",
        "play_device_count",
        "played_at_date_quarter",
    )
    .orderBy(F.col("ms_played").desc(), F.col("played_at_date").asc_nulls_last())
)

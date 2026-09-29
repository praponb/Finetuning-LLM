from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_latest = Window.partitionBy("source", "track_id").orderBy(F.col("ms_played").desc(), F.col("id").desc())
w_source = Window.partitionBy("source")
w_listener_id_by_played_at = Window.partitionBy("listener_id").orderBy("played_at", "id")
w_listener_id_by_played_at_tie = Window.partitionBy("listener_id").orderBy("played_at")

prepared_plays = (
    df_plays
    .filter((F.col("played_at") >= F.lit("2025-01-13 12:00:00").cast("timestamp")) & (F.col("listener_id").isNotNull()))
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "ms_played",
        "device",
        "is_skipped",
        "source",
        F.date_format(F.col("played_at"), "yyyy-MM").alias("played_at_label"),
    )
)

latest = (
    prepared_plays
    .withColumn("rn", F.row_number().over(w_latest))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

windowed = (
    latest
    .select(
        "*",
        F.round(F.col("ms_played") - F.avg("ms_played").over(w_source), 2).alias("ms_played_vs_source_avg"),
        F.row_number().over(w_listener_id_by_played_at).alias("rn"),
        F.round(F.percent_rank().over(w_listener_id_by_played_at_tie), 3).alias("played_at_percent_rank"),
        F.count(F.lit(1)).over(w_source).alias("rows_in_source"),
        F.lead("ms_played").over(w_listener_id_by_played_at).alias("next_ms_played"),
    )
)

result = (
    windowed
    .rollup("rn", "device")
    .agg(
        F.count("*").alias("row_count"),
        F.round(F.avg("played_at_percent_rank"), 2).alias("avg_played_at_percent_rank"),
        F.sum(F.when(F.col("device") == "speaker", 1).otherwise(0)).alias("n_device_speaker"),
    )
)

from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_dedup = Window.partitionBy("source", "is_skipped_label").orderBy("played_at", "id")
w_source_device = Window.partitionBy("source", "device")
w_device_by_played_at = Window.partitionBy("device").orderBy("played_at", "id")

cleaned = (
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
        F.when(F.col("is_skipped"), "yes").otherwise("no").alias("is_skipped_label"),
        F.concat_ws("-", F.col("source"), F.col("device")).alias("source_device_label"),
        F.concat(F.col("source"), F.lit("#"), F.col("id").cast("string")).alias("source_ref"),
    )
)

unique_rows = (
    cleaned
    .withColumn("recency_rank", F.row_number().over(w_dedup))
    .filter(F.col("recency_rank") == 1)
    .drop("recency_rank")
)

windowed = (
    unique_rows
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "device",
        "is_skipped",
        "source",
        "is_skipped_label",
        "source_ref",
        F.count(F.lit(1)).over(w_source_device).alias("rows_in_source_device"),
        F.round(F.col("ms_played") / F.nullif(F.sum("ms_played").over(w_source_device), F.lit(0)), 4).alias("ms_played_share_of_source_device"),
        F.rank().over(w_device_by_played_at).alias("played_at_rank"),
    )
)

result = (
    windowed
    .rollup("listener_id", "is_skipped")
    .agg(
        F.grouping("listener_id").alias("is_total_listener_id"),
        F.sum(F.when(~F.col("source").isin("radio", "album"), F.col("played_at_rank"))).alias("played_at_rank_source_not_in_radio_album"),
        F.min("played_at").alias("first_played_at"),
        F.count("*").alias("cnt"),
    )
    .orderBy(
        F.col("listener_id").asc_nulls_last(),
        F.col("is_skipped").asc_nulls_last(),
    )
)

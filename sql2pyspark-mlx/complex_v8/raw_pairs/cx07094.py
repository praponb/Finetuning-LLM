from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_source_device_by_played_at = Window.partitionBy("source", "device").orderBy(F.col("played_at").asc_nulls_last(), "id")

prepared_plays = (
    df_plays
    .filter((F.col("ms_played") >= 71476) & (F.col("source") == "playlist"))
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "device",
        "is_skipped",
        "source",
        F.when(F.col("is_skipped"), "yes").otherwise("no").alias("is_skipped_label"),
    )
)

sequenced = (
    prepared_plays
    .withColumn("played_at_rank", F.rank().over(w_source_device_by_played_at))
    .withColumn("minutes_since_prev_played_at", F.round((F.unix_timestamp("played_at") - F.unix_timestamp(F.lag("played_at").over(w_source_device_by_played_at))) / 60, 1))
)

totals = (
    sequenced
    .filter(F.col("minutes_since_prev_played_at") > 1000.0)
    .groupBy("played_at_rank", "device")
    .agg(
        F.sum(F.when(~F.col("is_skipped"), F.col("played_at_rank"))).alias("played_at_rank_is_skipped_false"),
        F.count("*").alias("row_count"),
        F.sum("played_at_rank").alias("total_played_at_rank"),
        F.max("played_at").alias("last_played_at"),
        F.round(F.sum("minutes_since_prev_played_at") / F.nullif(F.sum("played_at_rank"), F.lit(0)), 3).alias("minutes_since_prev_played_at_to_played_at_rank_r"),
        F.min("played_at").alias("first_played_at"),
    )
    .filter(F.col("row_count") >= 2)
)

result = (
    totals
    .groupBy("last_played_at")
    .pivot("device", ["speaker", "desktop", "tv"])
    .agg(F.min("played_at_rank_is_skipped_false"))
)

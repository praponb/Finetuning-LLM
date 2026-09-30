from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_device_is_skipped_by_played = Window.partitionBy("device", "is_skipped").orderBy("played_at", "id")
w_device_is_skipped_by_played_2 = Window.partitionBy("device", "is_skipped").orderBy("played_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_device_is_skipped_by_played_3 = Window.partitionBy("device", "is_skipped").orderBy("played_at", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_device_is_skipped_by_played_4 = Window.partitionBy("device", "is_skipped").orderBy("played_at")

scoped_plays = (
    df_plays
    .filter((F.col("ms_played") <= 318978) & (~F.col("is_skipped")))
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "ms_played",
        "device",
        "is_skipped",
        "source",
        F.to_date(F.col("played_at")).alias("played_at_date"),
        F.when(F.col("ms_played") >= 218129, "HIGH").when(F.col("ms_played") >= 73625, "MEDIUM").otherwise("LOW").alias("ms_played_tier"),
        F.length(F.trim(F.col("source"))).alias("source_len"),
    )
)

with_windows = (
    scoped_plays
    .select(
        "*",
        F.lead("played_at").over(w_device_is_skipped_by_played).alias("next_played_at"),
        F.count(F.lit(1)).over(w_device_is_skipped_by_played_2).alias("running_count"),
        F.round(F.cume_dist().over(w_device_is_skipped_by_played), 3).alias("played_at_cume_dist"),
        F.last("played_at").over(w_device_is_skipped_by_played_3).alias("last_played_at"),
        F.rank().over(w_device_is_skipped_by_played_4).alias("played_at_rank"),
    )
)

first_rows = (
    with_windows
    .filter((F.col("played_at_rank") <= 3) & (F.col("played_at_cume_dist") < 0.7))
    .drop("played_at_rank")
)

result = (
    first_rows
    .groupBy("track_id")
    .pivot("ms_played_tier", ["LOW", "HIGH"])
    .agg(F.min("source_len"))
    .orderBy("track_id")
)

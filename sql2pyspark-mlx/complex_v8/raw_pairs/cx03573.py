from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_dedup = Window.partitionBy("listener_id").orderBy(F.col("ms_played").desc(), F.col("id").desc())

base = (
    df_plays
    .select(
        "id",
        "listener_id",
        "track_id",
        "ms_played",
        "device",
        "is_skipped",
        F.abs(F.col("ms_played") - 245815).alias("ms_played_distance"),
    )
)

latest = (
    base
    .withColumn("rn", F.row_number().over(w_dedup))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

result = (
    latest
    .groupingSets(
        [["device", "is_skipped"], ["device"], ["is_skipped"], []],
        "device", "is_skipped",
    )
    .agg(
        F.round(F.avg("ms_played"), 2).alias("avg_ms_played"),
        F.countDistinct("id").alias("distinct_ids"),
        F.sum(F.when(F.col("is_skipped"), F.col("ms_played_distance"))).alias("ms_played_distance_is_skipped_true"),
    )
)

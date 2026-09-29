from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_latest = Window.partitionBy("device", "source").orderBy(F.col("played_at").desc(), F.col("id").desc())

recent = (
    df_plays
    .filter(~F.col("is_skipped"))
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "ms_played",
        "device",
        "is_skipped",
        "source",
        F.concat_ws("-", F.col("device"), F.col("source")).alias("device_source_label"),
    )
)

latest = (
    recent
    .withColumn("row_num", F.row_number().over(w_latest))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

scored = (
    latest
    .select(
        "id",
        "listener_id",
        "track_id",
        "ms_played",
        "device",
        "is_skipped",
        "source",
        F.when(F.col("source").isin("album", "search"), "group_a").when(F.col("source") == "radio", "group_b").otherwise("other").alias("source_group"),
    )
)

result = (
    scored
    .filter(F.col("is_skipped"))
    .select(
        "id",
        "listener_id",
        "track_id",
        "ms_played",
        "device",
        "source",
        "source_group",
        F.coalesce(F.col("ms_played"), F.lit(0)).alias("ms_played_filled"),
    )
    .orderBy(F.col("ms_played_filled").desc())
)

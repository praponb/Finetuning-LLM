from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_dedup = Window.partitionBy("source").orderBy(F.col("ms_played_bucket").desc(), F.col("id").desc())

base = (
    df_plays
    .select(
        "id",
        "listener_id",
        "track_id",
        "ms_played",
        "device",
        "is_skipped",
        "source",
    )
)

annotated = (
    base
    .select(
        "*",
        (F.floor(F.col("ms_played") / 50000) * 50000).alias("ms_played_bucket"),
        F.length(F.trim(F.col("device"))).alias("device_len"),
        F.length(F.trim(F.col("source"))).alias("source_len"),
    )
)

deduped = (
    annotated
    .withColumn("row_num", F.row_number().over(w_dedup))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

result = (
    deduped
    .select(
        "id",
        "listener_id",
        "track_id",
        "device",
        "is_skipped",
        "source",
        "ms_played_bucket",
        "device_len",
        "source_len",
        F.upper(F.trim(F.col("device"))).alias("device_clean"),
    )
    .orderBy(F.col("source_len").desc(), "id")
    .limit(20)
)

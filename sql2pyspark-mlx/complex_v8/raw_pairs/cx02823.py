from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_latest = Window.partitionBy("listener_id").orderBy(F.col("played_at").desc(), F.col("id").desc())

prepared = (
    df_plays
    .filter((F.col("track_id").isNotNull()) | (F.col("source") == "search"))
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "device",
        "source",
    )
)

deduped = (
    prepared
    .withColumn("rn", F.row_number().over(w_latest))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

result = (
    deduped
    .groupingSets(
        [["listener_id", "track_id", "source"], ["listener_id"], []],
        "listener_id", "track_id", "source",
    )
    .agg(
        F.grouping_id().alias("grouping_level"),
        F.count(F.when(F.col("device") == "speaker", 1)).alias("n_device_speaker"),
        F.count(F.when(F.col("source").isNotNull(), 1)).alias("n_source_is_not_null"),
        F.count(F.when(F.col("source") == "playlist", 1)).alias("n_source_playlist"),
    )
)

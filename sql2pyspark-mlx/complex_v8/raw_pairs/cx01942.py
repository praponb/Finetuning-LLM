from pyspark.sql import functions as F

base_plays = (
    df_plays
    .filter(F.col("is_skipped"))
    .select(
        "id",
        "listener_id",
        "track_id",
        "ms_played",
        "device",
        "is_skipped",
        F.length(F.trim(F.col("source"))).alias("source_len"),
    )
)

result = (
    base_plays
    .select("id", "source_len", "ms_played")
    .unpivot(
        ids=["id"],
        values=["source_len", "ms_played"],
        variableColumnName="measure",
        valueColumnName="reading",
    )
    .filter(F.col("reading").isNotNull())
    .orderBy("id", "measure")
)

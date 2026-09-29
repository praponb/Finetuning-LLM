from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_latest = Window.partitionBy("listener_id").orderBy("ms_played_filled", "id")
w_dedup = Window.partitionBy("listener_id").orderBy(F.col("played_at").desc(), F.col("id").desc())

filtered_plays = (
    df_plays
    .filter(F.col("played_at") < F.lit("2025-03-08 13:00:00").cast("timestamp"))
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "ms_played",
        "is_skipped",
        F.when(F.col("is_skipped"), 1).otherwise(0).alias("is_skipped_flag"),
        F.to_date(F.col("played_at")).alias("played_at_date"),
    )
)

scored = (
    filtered_plays
    .select(
        "id",
        "listener_id",
        "played_at",
        "ms_played",
        "is_skipped",
        "is_skipped_flag",
        "played_at_date",
        F.coalesce(F.col("ms_played"), F.lit(0)).alias("ms_played_filled"),
    )
)

latest = (
    scored
    .withColumn("rn", F.row_number().over(w_latest))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

deduped = (
    latest
    .withColumn("row_num", F.row_number().over(w_dedup))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

result = (
    deduped
    .select("id", "ms_played_filled", "is_skipped_flag")
    .unpivot(
        ids=["id"],
        values=["ms_played_filled", "is_skipped_flag"],
        variableColumnName="metric_name",
        valueColumnName="metric_value",
    )
    .filter(F.col("metric_value").isNotNull())
)

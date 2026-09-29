from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.year("event_time"), "page").orderBy("event_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy(F.year("event_time"), "session_id").orderBy(F.col("duration_sec").asc_nulls_first())
w3 = Window.partitionBy("session_id").orderBy("duration_sec", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("session_id").orderBy(F.col("duration_sec_cume_dist").desc(), "id")

metrics = (
    df_web_events
    .select(
        "id",
        "session_id",
        "page",
        "duration_sec",
        F.sum("duration_sec").over(w).alias("running_duration_sec"),
        F.cume_dist().over(w2).alias("duration_sec_cume_dist"),
    )
)

scored = (
    metrics
    .filter(F.col("session_id").isin([7, 16, 33, 35]))
    .withColumn("cumulative_running_duration_sec", F.sum("running_duration_sec").over(w3))
    .withColumn("row_seq", F.row_number().over(w4))
)

cleaned = (
    scored
    .filter(F.col("page").isNotNull() & (F.col("cumulative_running_duration_sec") < 5044))
    .select(
        "id",
        "session_id",
        "page",
        "running_duration_sec",
        "duration_sec_cume_dist",
        "cumulative_running_duration_sec",
        "row_seq",
        F.floor("cumulative_running_duration_sec").alias("cumulative_running_duration_sec_whole"),
        F.round("row_seq", 1).alias("row_seq_rounded"),
        F.coalesce("page", F.lit("UNKNOWN")).alias("page_clean"),
    )
)

remaining_keys = (
    cleaned
    .select("id", "page")
    .subtract(
        df_web_events
        .filter(F.col("session_id").isin([1, 14, 20]))
        .select("id", "page")
    )
)

verified = (
    cleaned
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.page") == F.col("k.page")), "inner")
    .select("s.*")
)

filtered = (
    verified
    .filter(F.col("row_seq_rounded") <= 46)
    .select(
        "id",
        "session_id",
        "page",
        "running_duration_sec",
        "duration_sec_cume_dist",
        "cumulative_running_duration_sec_whole",
        "row_seq_rounded",
        "page_clean",
        F.round((F.col("duration_sec_cume_dist") * 1.1), 2).alias("duration_sec_cume_dist_scaled"),
        (F.col("cumulative_running_duration_sec") + F.col("cumulative_running_duration_sec_whole")).alias("cumulative_running_duration_sec_plus_cumulative_running_duration_sec_whole"),
    )
)

result = (
    filtered
    .select(
        "session_id",
        "duration_sec_cume_dist_scaled",
        "row_seq_rounded",
        "cumulative_running_duration_sec_plus_cumulative_running_duration_sec_whole",
    )
    .unpivot(
        ids=["session_id"],
        values=["duration_sec_cume_dist_scaled", "row_seq_rounded", "cumulative_running_duration_sec_plus_cumulative_running_duration_sec_whole"],
        variableColumnName="measure",
        valueColumnName="reading",
    )
    .filter(F.col("reading").isNotNull())
)

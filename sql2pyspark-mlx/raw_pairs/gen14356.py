from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("event_type").orderBy("event_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("event_type").orderBy("duration_sec", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("session_id", "event_type").orderBy(F.col("cumulative_duration_sec_2").asc_nulls_last(), "id")
w4 = Window.partitionBy("page").orderBy(F.col("cumulative_duration_sec").asc_nulls_last())
w5 = Window.partitionBy("browser").orderBy(F.col("cumulative_duration_sec").desc_nulls_last())
w6 = Window.partitionBy("browser", "session_id")

scored = (
    df_web_events
    .withColumn("cumulative_duration_sec", F.sum("duration_sec").over(w))
    .withColumn("cumulative_duration_sec_2", F.sum("duration_sec").over(w2))
)

deduped = (
    scored
    .withColumn("latest_rank", F.row_number().over(w3))
    .filter(F.col("latest_rank") == 1)
)

ranked = (
    deduped
    .withColumn("rank_in_group", F.rank().over(w4))
    .filter(F.col("rank_in_group") == 1)
    .select(
        "id",
        "session_id",
        "event_type",
        "page",
        "event_time",
        "cumulative_duration_sec",
    )
)

joined = (
    ranked
    .alias("src")
    .join(df_sessions.alias("s"), F.col("src.session_id") == F.col("s.id"), "inner")
    .select("src.*", F.col("s.browser"))
)

with_windows = (
    joined
    .select(
        "id",
        "session_id",
        "page",
        "cumulative_duration_sec",
        F.cume_dist().over(w5).alias("cumulative_duration_sec_cume_dist"),
        F.min("cumulative_duration_sec").over(w6).alias("browser_min_cumulative_duration_sec"),
    )
)

result = (
    with_windows
    .groupBy("session_id")
    .agg(
        F.count(F.when(F.col("page") == "/home", 1)).alias("home_count"),
        F.round(F.avg("cumulative_duration_sec_cume_dist"), 2).alias("avg_cumulative_duration_sec_cume_dist"),
        F.percentile_approx("cumulative_duration_sec", 0.5).alias("cumulative_duration_sec_p50"),
    )
    .orderBy(F.col("home_count").desc_nulls_first())
)

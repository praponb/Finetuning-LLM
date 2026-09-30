from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("session_id").orderBy(F.col("event_time").desc(), "id")
w2 = Window.partitionBy("page", "browser").orderBy(F.col("duration_sec").desc_nulls_first(), "id")

with_dims = (
    df_web_events
    .alias("a")
    .join(df_sessions.alias("p"), F.col("a.session_id") == F.col("p.id"), "left")
    .filter(F.col("p.browser").isin(["Chrome", "Edge"]))
    .select("a.*", F.col("p.device"), F.col("p.country"), F.col("p.browser"))
)

deduped = (
    with_dims
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
)

filtered = (
    deduped
    .filter(F.col("event_time") > F.lit("2023-07-06 07:00:00").cast("timestamp"))
    .select(
        "*",
        F.coalesce("browser", F.lit("unassigned")).alias("browser_filled"),
        F.coalesce("browser", F.lit("DIRECT")).alias("resolved_browser"),
    )
)

deduped_web_events = (
    filtered
    .withColumn("dedup_rank", F.row_number().over(w2))
    .filter(F.col("dedup_rank") == 1)
)

result = (
    deduped_web_events
    .groupBy("session_id")
    .agg(
        F.count(F.when(F.col("country") == "UK", 1)).alias("uk_count"),
        F.count(F.when(F.col("country") == "UK", 1)).alias("uk_count_2"),
        F.size(F.collect_set("browser")).alias("num_browsers"),
    )
    .filter(F.col("num_browsers") >= 1)
)

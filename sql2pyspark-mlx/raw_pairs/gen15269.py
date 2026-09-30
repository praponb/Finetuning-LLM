from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("event_type").orderBy("event_time", "id").rowsBetween(-5, Window.currentRow)
w2 = Window.partitionBy("page").orderBy("duration_sec", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
duration_sec_by_session_id = df_web_events.groupBy("session_id").agg(F.sum("duration_sec").alias("sum_web_event_duration_sec"))

base = (
    df_web_events
    .filter(F.col("page").isNotNull())
    .withColumn("duration_sec_band", F.when(F.col("duration_sec") >= 511, "High").when(F.col("duration_sec") >= 155, "Medium").otherwise("Low"))
    .withColumn("page_clean", F.coalesce("page", F.lit("OTHER")))
)

with_windows = (
    base
    .select(
        "*",
        F.avg("duration_sec").over(w).alias("duration_sec_avg_5"),
        F.first("page", ignorenulls=True).over(w2).alias("first_known_page"),
    )
)

with_dims = (
    with_windows
    .alias("s")
    .join(df_sessions.alias("p2"), F.col("s.session_id") == F.col("p2.id"), "left")
    .select("s.*", F.col("p2.country"), F.col("p2.device"), F.col("p2.browser"))
)

enriched = (
    with_dims
    .alias("src")
    .join(df_sessions.alias("p"), F.col("src.session_id") == F.col("p.id"), "inner")
    .select(
        "src.id",
        "src.session_id",
        "src.event_type",
        "src.event_time",
        "src.page_clean",
        "src.duration_sec_avg_5",
        "src.country",
        F.col("p.device").alias("session_device"),
        F.col("p.country").alias("session_country"),
    )
)

result = (
    enriched
    .join(duration_sec_by_session_id, on="session_id", how="left")
    .select(
        "id",
        "session_id",
        "event_type",
        "page_clean",
        "country",
        "session_device",
        F.col("sum_web_event_duration_sec"),
    )
    .orderBy(F.col("id").desc(), "session_id")
)

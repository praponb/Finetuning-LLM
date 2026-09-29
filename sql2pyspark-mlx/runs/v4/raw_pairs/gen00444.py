from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("browser").orderBy("duration_sec", "id")
w2 = Window.partitionBy("device").orderBy(F.col("duration_sec").desc(), "id")
w3 = Window.partitionBy("browser").orderBy(F.col("duration_sec").desc(), "id")

base = (
    df_web_events
    .filter(F.col("event_time") > F.lit("2024-04-02").cast("date"))
    .select(
        "id",
        "session_id",
        "event_type",
        "page",
        "event_time",
        "duration_sec",
        F.substring("page", 1, 4).alias("page_prefix"),
        F.coalesce("page", F.lit("NONE")).alias("resolved_page"),
    )
)

with_dims = (
    base
    .alias("e")
    .join(df_sessions.alias("b"), F.col("e.session_id") == F.col("b.id"), "left")
    .select("e.*", F.col("b.browser"), F.col("b.country"), F.col("b.device"))
)

base_web_events = (
    with_dims
    .filter(F.col("country").isin(["US", "TH", "JP"]))
    .select(
        "id",
        "session_id",
        "event_type",
        "page",
        "event_time",
        "duration_sec",
        "page_prefix",
        "resolved_page",
        "browser",
        "device",
        F.when(F.col("event_type").isin(["purchase", "page_view"]), "Priority").otherwise("Other").alias("event_type_group"),
    )
)

result = (
    base_web_events
    .select(
        "*",
        F.lag("duration_sec", 1).over(w).alias("prev_duration_sec"),
        F.ntile(5).over(w2).alias("duration_sec_bucket"),
        F.ntile(5).over(w3).alias("duration_sec_bucket_2"),
    )
)

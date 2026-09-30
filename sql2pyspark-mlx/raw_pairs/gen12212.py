from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_hist = Window.partitionBy("event_type").orderBy("duration_sec", "id")
w_running = Window.partitionBy(F.to_date("event_time")).orderBy(F.col("prev_duration_sec_rounded").desc(), "id")
w_ordered = Window.partitionBy("event_type").orderBy(F.col("event_time").desc(), "id")

base = (
    df_web_events
    .filter((F.col("event_time") > F.lit("2023-10-18 11:00:00").cast("timestamp")) & F.col("page").isNotNull())
    .withColumn("duration_sec_rounded", F.round("duration_sec", 2))
    .withColumn("page_filled", F.coalesce("page", F.lit("N/A")))
)

metrics = (
    base
    .filter(F.col("duration_sec").between(219, 344))
    .select(
        "id",
        "session_id",
        "event_type",
        "page",
        "event_time",
        "page_filled",
        F.lag("duration_sec_rounded").over(w_hist).alias("prev_duration_sec_rounded"),
        F.lag("duration_sec").over(w_hist).alias("prev_duration_sec"),
        F.row_number().over(w_hist).alias("row_seq"),
    )
)

enriched = (
    metrics
    .select(
        "*",
        F.row_number().over(w_running).alias("seq_no"),
        F.row_number().over(w_ordered).alias("event_seq"),
    )
)

enriched_web_events = (
    enriched
    .alias("e")
    .join(df_sessions.alias("p"), F.col("e.session_id") == F.col("p.id"), "inner")
    .select(
        "e.id",
        "e.session_id",
        "e.page",
        "e.event_time",
        "e.page_filled",
        "e.prev_duration_sec",
        "e.seq_no",
        "e.event_seq",
        F.col("p.browser"),
        F.col("p.device"),
    )
)

remaining_keys = (
    enriched_web_events
    .select("page")
    .subtract(
        df_web_events
        .filter(F.col("duration_sec").between(235, 470))
        .select("page")
    )
)

verified = (
    enriched_web_events
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.page") == F.col("k.page")), "inner")
    .select("s.*")
)

with_dims = (
    verified
    .alias("a")
    .join(df_sessions.alias("b"), F.col("a.session_id") == F.col("b.id"), "inner")
    .select(
        "a.session_id",
        "a.page",
        "a.page_filled",
        "a.prev_duration_sec",
        "a.event_seq",
        "a.browser",
        "a.device",
        F.col("b.device").alias("session_device"),
        F.col("b.browser").alias("session_browser"),
    )
)

with_dims_web_events = (
    with_dims
    .alias("s")
    .join(df_sessions.alias("p2"), F.col("s.session_id") == F.col("p2.id"), "left")
    .select(
        "s.session_id",
        "s.page_filled",
        "s.prev_duration_sec",
        "s.browser",
        "s.session_device",
        F.col("p2.country"),
        F.col("p2.browser").alias("session_browser_2"),
    )
)

stats = (
    with_dims_web_events
    .groupBy(
        F.when(F.col("prev_duration_sec") >= 398, "HIGH").otherwise("LOW").alias("prev_duration_sec_tier"),
    )
    .agg(
        F.round(F.sum("prev_duration_sec") / F.nullif(F.count(F.when(F.col("country") == "DE", 1)), F.lit(0)), 2).alias("prev_duration_sec_per_de"),
        F.count(F.when(F.col("session_browser_2") == "Edge", 1)).alias("edge_count"),
        F.min("prev_duration_sec").alias("min_prev_duration_sec"),
        F.round(F.sum("prev_duration_sec") / F.nullif(F.count(F.when(F.col("browser") == "Edge", 1)), F.lit(0)), 2).alias("prev_duration_sec_per_edge"),
    )
)

result = (
    stats
    .select(
        "prev_duration_sec_tier",
        "edge_count",
        "min_prev_duration_sec",
        "prev_duration_sec_per_edge",
    )
)

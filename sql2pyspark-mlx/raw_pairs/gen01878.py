from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.duration_sec").desc(), F.col("c.id").desc())
w2 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.event_time").desc(), F.col("c.id").desc())

latest_match = (
    df_sessions
    .alias("s")
    .join(df_web_events.alias("c"), (F.col("c.session_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.event_time"))
)

latest_match_sessions = (
    latest_match
    .alias("s")
    .join(df_web_events.alias("c"), (F.col("c.session_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w2))
    .filter(F.col("rn") == 1)
    .select("s.id", "s.browser", F.col("c.page").alias("web_event_page"))
)

not_flagged = (
    latest_match_sessions
    .alias("s")
    .join(df_web_events.alias("c"), (F.col("c.session_id") == F.col("s.id")) & (F.col("c.page").isin(["/home", "/cart", "/checkout"])), "left_anti")
)

cleaned = (
    not_flagged
    .filter(F.col("web_event_page").isin(["/product", "/checkout"]))
    .select(
        "*",
        F.concat_ws("/", "browser", "web_event_page").alias("browser_web_event_page_key"),
        F.coalesce("browser", F.lit("OTHER")).alias("browser_clean"),
    )
)

with_last = (
    cleaned
    .alias("s")
    .join(df_web_events.alias("c"), (F.col("c.session_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.browser",
        "s.web_event_page",
        "s.browser_web_event_page_key",
        F.col("c.event_time"),
        F.col("c.event_type"),
    )
)

stats = (
    with_last
    .groupBy(F.date_trunc("YEAR", "event_time").alias("year_start"), "event_type")
    .agg(
        F.max("event_time").alias("last_seen"),
        F.min("event_time").alias("started_at"),
        F.size(F.collect_set("web_event_page")).alias("num_web_event_pages"),
        F.countDistinct("web_event_page").alias("distinct_web_event_pages"),
    )
)

result = (
    stats
    .select("year_start", "event_type", "last_seen", "started_at")
    .orderBy("event_type")
)

from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.event_time").desc(), F.col("c.id").desc())
w2 = Window.partitionBy("web_event_event_type").orderBy("started_at", "id")
w3 = Window.partitionBy("web_event_event_type").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

cleaned = (
    df_sessions
    .filter(F.col("device") == "mobile")
    .select(
        "id",
        "customer_id",
        "browser",
        "started_at",
        "country",
        F.substring("browser", 1, 2).alias("browser_prefix"),
    )
)

with_latest = (
    cleaned
    .alias("s")
    .join(df_web_events.alias("c"), (F.col("c.session_id") == F.col("s.id")) & (F.col("c.event_time") <= F.col("s.started_at")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select(
        "s.*",
        F.col("c.event_type").alias("web_event_event_type"),
        F.col("c.event_time").alias("web_event_event_time"),
    )
)

eligible = (
    with_latest
    .alias("s")
    .join(df_web_events.alias("x"), (F.col("x.session_id") == F.col("s.id")) & (F.col("x.event_time") <= F.col("s.web_event_event_time")), "left_anti")
)

prev_events = eligible.withColumn("prev_started_at", F.lag("started_at").over(w2))

gaps = (
    prev_events
    .withColumn("is_new_burst", F.when(F.col("prev_started_at").isNull() | (F.col("started_at") > F.col("prev_started_at") + F.expr("INTERVAL 30 DAYS")), 1).otherwise(0))
)

sessionized = (
    gaps
    .select(
        "id",
        "customer_id",
        "started_at",
        "country",
        "web_event_event_type",
        "web_event_event_time",
        F.sum("is_new_burst").over(w3).alias("session_id"),
    )
)

enriched = (
    sessionized
    .alias("e")
    .join(df_customers.alias("p"), F.col("e.customer_id") == F.col("p.id"), "inner")
    .filter(F.col("p.credit_limit") < 6400)
    .select("e.*", F.col("p.segment"), F.col("p.city"))
)

result = (
    enriched
    .alias("e")
    .join(df_customers.alias("dim"), F.col("e.customer_id") == F.col("dim.id"), "inner")
    .cube(
        F.col("dim.city").alias("customer_city"),
        F.col("dim.segment").alias("customer_segment"),
    )
    .agg(
        F.count("*").alias("rows_total"),
        F.max(F.col("e.session_id")).alias("max_session_id"),
        F.grouping_id().alias("gid"),
    )
)

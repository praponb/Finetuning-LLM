from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.duration_sec").desc(), F.col("c.id").desc())
w2 = Window.partitionBy("web_event_event_type").orderBy("started_at", "id")
w3 = Window.partitionBy("web_event_event_type").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("country", "city").orderBy(F.col("started_at").desc(), "id")
w5 = Window.partitionBy(F.date_trunc("MONTH", "started_at"), "page")
w6 = Window.partitionBy("page").orderBy("session_id", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w7 = Window.partitionBy("country").orderBy("dedup_rank", "id")

base = (
    df_sessions
    .filter(F.col("started_at") >= F.to_timestamp(F.lit("2023-03-01 14:00:00")))
    .select("*", F.date_format("started_at", "yyyy-MM-dd").alias("started_at_label"))
)

latest_match = (
    base
    .alias("s")
    .join(df_web_events.alias("c"), (F.col("c.session_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.customer_id",
        "s.device",
        "s.browser",
        "s.started_at",
        F.col("c.event_time").alias("web_event_event_time"),
        F.col("c.event_type").alias("web_event_event_type"),
    )
)

kept = (
    latest_match
    .alias("s")
    .join(df_web_events.alias("ch"), (F.col("ch.session_id") == F.col("s.id")) & (F.col("ch.event_time") <= F.col("s.web_event_event_time")), "left_anti")
)

enriched = (
    kept
    .alias("s")
    .join(df_customers.alias("d"), F.col("s.customer_id") == F.col("d.id"), "inner")
    .filter(F.col("d.city").isin(["London", "Singapore", "Tokyo"]))
    .select("s.*", F.col("d.country"), F.col("d.city"))
)

gaps = (
    enriched
    .withColumn("session_start_flag", F.when(F.col("started_at") - F.lag("started_at").over(w2) > F.expr("INTERVAL 60 DAYS"), 1).otherwise(0))
)

sessions_sessions = gaps.withColumn("session_id", F.sum("session_start_flag").over(w3))

latest_match_sessions = (
    sessions_sessions
    .alias("s")
    .join(df_web_events.alias("c"), (F.col("c.session_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.customer_id",
        "s.started_at",
        "s.country",
        "s.city",
        "s.session_id",
        F.col("c.page"),
    )
)

deduped = (
    latest_match_sessions
    .withColumn("dedup_rank", F.row_number().over(w4))
    .filter(F.col("dedup_rank") == 1)
)

result = (
    deduped
    .select(
        "id",
        "customer_id",
        "country",
        "page",
        F.count("session_id").over(w5).alias("started_at_period_count_session_id"),
        F.sum("session_id").over(w6).alias("running_session_id"),
        F.lead("session_id", 1).over(w7).alias("next_session_id"),
    )
)

from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("browser").orderBy("started_at", "id")
w2 = Window.partitionBy("browser").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("customer_id", "country").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.orderBy("session_id", "id")
w5 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.event_time").desc(), F.col("c.id").desc())
w6 = Window.partitionBy("web_event_event_type").orderBy("session_id_bucket", "id").rowsBetween(-6, -1)
w7 = Window.partitionBy("web_event_event_type").orderBy("session_id_bucket", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

with_gaps = (
    df_sessions
    .withColumn("is_new_session", F.when(F.lag("started_at").over(w).isNull() | (F.col("started_at") > F.lag("started_at").over(w) + F.expr("INTERVAL 90 DAYS")), 1).otherwise(0))
)

sessions_sessions = (
    with_gaps
    .select(
        "id",
        "customer_id",
        "browser",
        "started_at",
        "country",
        F.sum("is_new_session").over(w2).alias("session_id"),
    )
)

with_windows = (
    sessions_sessions
    .filter(F.hour("started_at").between(10, 14))
    .select(
        "id",
        "customer_id",
        "browser",
        "session_id",
        F.max("session_id").over(w3).alias("running_max_session_id"),
        F.count("session_id").over(w3).alias("running_count_session_id"),
        F.sum("session_id").over(w3).alias("running_sum_session_id"),
        F.min("session_id").over(w3).alias("running_min_session_id"),
        F.ntile(3).over(w4).alias("session_id_bucket"),
    )
)

latest_match = (
    with_windows
    .alias("s")
    .join(df_web_events.alias("c"), (F.col("c.session_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w5))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.customer_id",
        "s.browser",
        "s.session_id_bucket",
        F.col("c.event_type").alias("web_event_event_type"),
    )
)

eligible = (
    latest_match
    .alias("s")
    .join(df_web_events.alias("ch"), (F.col("ch.session_id") == F.col("s.id")) & (F.col("ch.event_type").isin(["search", "add_to_cart", "click"])), "left_anti")
)

scored = (
    eligible
    .select(
        "id",
        "customer_id",
        "browser",
        "web_event_event_type",
        F.avg("session_id_bucket").over(w6).alias("session_id_bucket_avg_6_prior"),
        F.sum("session_id_bucket").over(w7).alias("running_session_id_bucket"),
    )
)

result = (
    scored
    .select(
        "id",
        "browser",
        "web_event_event_type",
        "session_id_bucket_avg_6_prior",
        "running_session_id_bucket",
        F.when(F.col("browser").rlike("^(Safari|Edge)$"), "EXTENDED").when(F.col("browser").rlike("^(Chrome|Firefox)$"), "PRIORITY").otherwise("OTHER_ZONE").alias("browser_zone"),
    )
)

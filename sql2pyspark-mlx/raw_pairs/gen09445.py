from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.event_time").desc(), F.col("c.id").desc())
w_recent = Window.partitionBy("event_type").orderBy("web_event_duration_sec", "id")
w3 = Window.partitionBy("browser").orderBy("web_event_duration_sec", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_running = Window.partitionBy("country").orderBy("prev_web_event_duration_sec", "id").rowsBetween(-6, Window.currentRow)
w5 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.duration_sec").desc(), F.col("c.id").desc())

valid = (
    df_sessions
    .filter(F.col("browser").isNotNull() & (~F.col("browser").eqNullSafe(F.lit("Firefox"))))
    .select(
        "*",
        F.when(F.col("device").isin(["desktop", "mobile"]), "Group A").otherwise("Other").alias("device_group"),
        F.coalesce("browser", F.lit("OTHER")).alias("browser_clean"),
    )
)

with_last = (
    valid
    .alias("s")
    .join(df_web_events.alias("c"), (F.col("c.session_id") == F.col("s.id")) & (F.col("c.event_time") <= F.col("s.started_at")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.browser",
        "s.country",
        "s.device_group",
        "s.browser_clean",
        F.col("c.duration_sec").alias("web_event_duration_sec"),
        F.col("c.event_type"),
    )
)

enriched = (
    with_last
    .select(
        "id",
        "country",
        "web_event_duration_sec",
        F.lead("web_event_duration_sec").over(w_recent).alias("next_web_event_duration_sec"),
        F.lag("web_event_duration_sec").over(w_recent).alias("prev_web_event_duration_sec"),
        F.row_number().over(w_recent).alias("seq_no"),
        F.sum("web_event_duration_sec").over(w3).alias("running_web_event_duration_sec"),
    )
)

scored = (
    enriched
    .select(
        "id",
        "web_event_duration_sec",
        "next_web_event_duration_sec",
        "prev_web_event_duration_sec",
        "running_web_event_duration_sec",
        F.max("running_web_event_duration_sec").over(w_running).alias("last7_max_running_web_event_duration_sec"),
        F.avg("running_web_event_duration_sec").over(w_running).alias("last7_avg_running_web_event_duration_sec"),
        F.sum("running_web_event_duration_sec").over(w_running).alias("last7_sum_running_web_event_duration_sec"),
    )
)

latest_match = (
    scored
    .alias("s")
    .join(df_web_events.alias("c"), (F.col("c.session_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w5))
    .filter(F.col("rn") == 1)
    .select(
        "s.*",
        F.col("c.event_type"),
        F.col("c.event_time").alias("web_event_event_time"),
    )
)

with_latest = (
    latest_match
    .alias("s")
    .join(df_web_events.alias("c"), (F.col("c.session_id") == F.col("s.id")) & (F.col("c.event_time") <= F.col("s.web_event_event_time")), "inner")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.prev_web_event_duration_sec",
        "s.running_web_event_duration_sec",
        "s.last7_avg_running_web_event_duration_sec",
        "s.event_type",
        "s.web_event_event_time",
        F.col("c.page").alias("web_event_page"),
    )
)

result = (
    with_latest
    .groupBy(F.date_trunc("WEEK", "web_event_event_time").alias("week_start"))
    .agg(
        F.percentile_approx("last7_avg_running_web_event_duration_sec", 0.9).alias("last7_avg_running_web_event_duration_sec_p90"),
        F.count(F.when(F.col("web_event_page") == "/product", 1)).alias("product_rows"),
        F.sum(F.when(F.col("web_event_page") == "/cart", F.col("running_web_event_duration_sec")).otherwise(0)).alias("cart_running_web_event_duration_sec"),
        F.percentile_approx("last7_avg_running_web_event_duration_sec", 0.95).alias("last7_avg_running_web_event_duration_sec_p95"),
    )
    .orderBy(F.col("cart_running_web_event_duration_sec").desc())
)

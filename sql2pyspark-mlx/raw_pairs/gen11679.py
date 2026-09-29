from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.event_time").desc(), F.col("c.id").desc())
w2 = Window.orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy(F.to_date("started_at"), "customer_id").orderBy("started_at", "id").rowsBetween(-6, -1)
w4 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.duration_sec").desc(), F.col("c.id").desc())
w5 = Window.orderBy("age_avg_6_prior", "id")
w6 = Window.partitionBy("event_type", "page").orderBy(F.col("web_event_duration_sec").desc_nulls_first())
w7 = Window.orderBy("web_event_duration_sec", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

valid = (
    df_sessions
    .filter(F.col("customer_id").isNotNull() & (F.col("started_at") >= F.lit("2023-06-04 04:00:00").cast("timestamp")))
    .select(
        "*",
        F.concat("device", F.lit("/"), "browser").alias("device_browser"),
        F.coalesce("browser", F.lit("N/A")).alias("browser_filled"),
    )
)

enriched = (
    valid
    .alias("s")
    .join(df_customers.alias("d"), F.col("s.customer_id") == F.col("d.id"), "left")
    .filter(F.col("d.email").like("%@gmail.com"))
    .select(
        "s.id",
        "s.customer_id",
        "s.device",
        "s.browser",
        "s.started_at",
        "s.browser_filled",
        F.col("d.age"),
        F.col("d.name"),
    )
)

with_last = (
    enriched
    .alias("s")
    .join(df_web_events.alias("c"), (F.col("c.session_id") == F.col("s.id")) & (F.col("c.event_time") <= F.col("s.started_at")), "inner")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.event_type"))
)

with_windows = (
    with_last
    .select(
        "id",
        "customer_id",
        "started_at",
        "name",
        "event_type",
        F.last("event_type", ignorenulls=True).over(w2).alias("last_known_event_type"),
        F.avg("age").over(w3).alias("age_avg_6_prior"),
    )
)

with_latest = (
    with_windows
    .alias("s")
    .join(df_web_events.alias("c"), (F.col("c.session_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w4))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.name",
        "s.event_type",
        "s.last_known_event_type",
        "s.age_avg_6_prior",
        F.col("c.duration_sec").alias("web_event_duration_sec"),
        F.col("c.page"),
    )
)

scored = (
    with_latest
    .withColumn("seq_no", F.row_number().over(w5))
    .withColumn("web_event_duration_sec_cume_dist", F.cume_dist().over(w6))
    .withColumn("cumulative_web_event_duration_sec", F.sum("web_event_duration_sec").over(w7))
)

cleaned = (
    scored
    .filter(F.col("page").isNotNull() & (F.col("page").eqNullSafe(F.lit("/checkout"))))
    .select(
        "*",
        (F.col("cumulative_web_event_duration_sec") * F.col("web_event_duration_sec")).alias("cumulative_web_event_duration_sec_times_web_event_duration_sec"),
    )
)

result = (
    cleaned
    .groupBy("page")
    .pivot("event_type", ["search", "click"])
    .agg(
        F.max("cumulative_web_event_duration_sec").alias("amount"),
        F.count(F.lit(1)).alias("cnt"),
    )
)

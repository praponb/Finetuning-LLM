from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy(F.col("started_at").desc_nulls_first(), "id")
w2 = Window.partitionBy("country").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("device", "browser").orderBy("started_at", "id")
w4 = Window.orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.event_time").desc(), F.col("c.id").desc())
w6 = Window.partitionBy("customer_id").orderBy("started_at", "id").rowsBetween(-3, Window.currentRow)
w7 = Window.partitionBy("device").orderBy("web_event_duration_sec", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

base = (
    df_sessions
    .filter((F.col("started_at") >= F.lit("2023-05-11").cast("date")) & (F.col("country").eqNullSafe(F.lit("JP"))))
    .select(
        "id",
        "customer_id",
        "device",
        "browser",
        "started_at",
        "country",
        F.concat_ws("/", "device", "browser").alias("device_browser_key"),
        F.upper("browser").alias("browser_upper"),
    )
)

latest = (
    base
    .withColumn("latest_rank", F.row_number().over(w))
    .filter(F.col("latest_rank") == 1)
    .select(
        "id",
        "customer_id",
        "device",
        "browser",
        "started_at",
        "country",
        "browser_upper",
    )
)

scored = (
    latest
    .withColumn("last_known_browser", F.last("browser", ignorenulls=True).over(w2))
    .withColumn("event_seq", F.row_number().over(w3))
    .withColumn("first_known_browser", F.first("browser", ignorenulls=True).over(w4))
)

with_last = (
    scored
    .alias("s")
    .join(df_web_events.alias("c"), (F.col("c.session_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w5))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.customer_id",
        "s.device",
        "s.started_at",
        "s.browser_upper",
        "s.last_known_browser",
        F.col("c.duration_sec").alias("web_event_duration_sec"),
    )
)

result = (
    with_last
    .filter(F.col("customer_id").isin([3, 22, 30]))
    .select(
        "id",
        "device",
        "browser_upper",
        "last_known_browser",
        F.avg("web_event_duration_sec").over(w6).alias("last4_avg_web_event_duration_sec"),
        F.min("web_event_duration_sec").over(w6).alias("last4_min_web_event_duration_sec"),
        F.count("web_event_duration_sec").over(w6).alias("last4_count_web_event_duration_sec"),
        F.sum("web_event_duration_sec").over(w7).alias("running_web_event_duration_sec"),
    )
)

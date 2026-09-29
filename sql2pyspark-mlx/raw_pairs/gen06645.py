from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.to_date("started_at"), "device").orderBy("started_at", "id")
w_hist = Window.partitionBy("country").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy(F.date_trunc("MONTH", "started_at"), "customer_id").orderBy(F.col("started_at").desc(), "id")
w4 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.event_time").desc(), F.col("c.id").desc())
w5 = Window.partitionBy("country").orderBy("web_event_duration_sec", "id")
w6 = Window.partitionBy("country").orderBy("event_seq", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w7 = Window.partitionBy("customer_id", "device")
w8 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.duration_sec").desc(), F.col("c.id").desc())

metrics = (
    df_sessions
    .filter(F.col("country") == "JP")
    .select(
        "id",
        "customer_id",
        "device",
        "browser",
        "country",
        F.row_number().over(w).alias("event_seq"),
        F.last("browser", ignorenulls=True).over(w_hist).alias("last_known_browser"),
        F.last("browser", ignorenulls=True).over(w_hist).alias("last_known_browser_2"),
        F.row_number().over(w3).alias("seq_no"),
    )
)

with_latest = (
    metrics
    .alias("s")
    .join(df_web_events.alias("c"), (F.col("c.session_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w4))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.duration_sec").alias("web_event_duration_sec"))
)

latest = (
    with_latest
    .withColumn("dedup_rank", F.row_number().over(w5))
    .filter(F.col("dedup_rank") == 1)
)

metrics_sessions = (
    latest
    .select(
        "*",
        F.sum("web_event_duration_sec").over(w6).alias("running_web_event_duration_sec"),
        F.max("seq_no").over(w7).alias("customer_id_max_seq_no"),
    )
)

latest_match = (
    metrics_sessions
    .alias("s")
    .join(df_web_events.alias("c"), (F.col("c.session_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w8))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.event_type").alias("web_event_event_type"))
)

matched_keys = (
    latest_match
    .select("country", "id")
    .exceptAll(
        df_sessions
        .filter(F.col("started_at") < "2023-09-29")
        .select("country", "id")
    )
)

reconciled = (
    latest_match
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.country") == F.col("k.country")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

result = (
    reconciled
    .alias("e")
    .join(df_customers.alias("dim"), F.col("e.customer_id") == F.col("dim.id"), "left")
    .rollup(
        F.col("dim.country").alias("customer_country"),
        F.col("e.browser").alias("browser"),
    )
    .agg(
        F.count("*").alias("num_rows"),
        F.max(F.col("e.web_event_duration_sec")).alias("max_web_event_duration_sec"),
        F.sum(F.col("e.running_web_event_duration_sec")).alias("total_running_web_event_duration_sec"),
        F.grouping_id().alias("level_id"),
    )
)

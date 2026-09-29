from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("session_id").orderBy("event_time", "id")

filtered = (
    df_web_events
    .filter(F.col("session_id").isin([21, 29, 38, 40]))
    .select(
        "id",
        "session_id",
        "event_type",
        "page",
        "event_time",
        "duration_sec",
        F.upper("event_type").alias("event_type_upper"),
    )
)

enriched = (
    filtered
    .alias("a")
    .join(df_sessions.alias("p"), F.col("a.session_id") == F.col("p.id"), "left")
    .select("a.*", F.col("p.country"))
)

valid = (
    enriched
    .filter(F.col("page").isNotNull() & F.col("page").isNotNull())
    .select(
        "id",
        "session_id",
        "event_type",
        "page",
        "event_time",
        "duration_sec",
        "event_type_upper",
        "country",
        F.col("duration_sec").cast("string").alias("duration_sec_str"),
    )
)

ranked = (
    valid
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

valid_web_events = (
    ranked
    .filter(F.col("event_type").isin(["search", "add_to_cart", "purchase"]))
    .select("*", F.date_trunc("WEEK", "event_time").alias("event_time_period"))
)

result = (
    valid_web_events
    .groupBy("page")
    .agg(
        F.size(F.collect_set("page")).alias("num_pages"),
        F.percentile_approx("duration_sec", 0.95).alias("duration_sec_p95"),
        ((F.unix_timestamp(F.max("event_time")) - F.unix_timestamp(F.min("event_time"))) / 60).alias("span_minutes"),
        F.max("event_time").alias("last_seen"),
        F.stddev_samp("duration_sec").alias("duration_sec_stddev"),
    )
    .orderBy("span_minutes")
)

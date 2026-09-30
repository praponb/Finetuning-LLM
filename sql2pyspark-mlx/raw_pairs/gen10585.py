from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("session_id").orderBy("event_time", "id")
w2 = Window.partitionBy("session_id").orderBy("event_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("page", "event_type").orderBy("session_no", "id")
w_part = Window.partitionBy("browser", "page").orderBy(F.col("next_duration_sec").asc_nulls_first())

filtered = (
    df_web_events
    .filter(F.col("page").isNotNull() & (F.col("event_time") >= F.to_timestamp(F.lit("2023-03-13 06:00:00"))))
    .withColumn("event_type_length", F.length("event_type"))
)

flagged = (
    filtered
    .withColumn("is_new_burst", F.when(F.lag("event_time").over(w).isNull() | (F.col("event_time") > F.lag("event_time").over(w) + F.expr("INTERVAL 7 DAYS")), 1).otherwise(0))
)

sessions_web_events = flagged.withColumn("session_no", F.sum("is_new_burst").over(w2))

with_windows = (
    sessions_web_events
    .select(
        "*",
        F.lag("session_no").over(w3).alias("prev_session_no"),
        F.lag("duration_sec").over(w3).alias("prev_duration_sec"),
        F.lead("duration_sec").over(w3).alias("next_duration_sec"),
        F.row_number().over(w3).alias("seq_no"),
    )
)

joined = (
    with_windows
    .alias("src")
    .join(df_sessions.alias("p"), F.col("src.session_id") == F.col("p.id"), "left")
    .select("src.*", F.col("p.device"), F.col("p.browser"), F.col("p.country"))
)

matched_keys = (
    joined
    .select("session_id", "id")
    .subtract(
        df_web_events
        .filter(~F.col("page").isin(["/checkout", "/search"]))
        .select("session_id", "id")
    )
)

verified = (
    joined
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.session_id") == F.col("k.session_id")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

scored = (
    verified
    .select(
        "event_type",
        "page",
        "duration_sec",
        "session_no",
        "prev_session_no",
        "country",
        F.rank().over(w_part).alias("next_duration_sec_rank"),
    )
)

valid = (
    scored
    .filter(F.col("page").isNotNull())
    .select(
        "event_type",
        "page",
        "duration_sec",
        "session_no",
        "prev_session_no",
        "country",
        F.initcap(F.trim("page")).alias("page_title"),
        F.coalesce("page", F.lit("N/A")).alias("page_filled"),
    )
)

result = (
    valid
    .rollup("prev_session_no")
    .agg(
        F.countDistinct("event_type").alias("distinct_event_types"),
        F.size(F.collect_set("country")).alias("num_countries"),
        F.sum(F.when(F.col("country") == "DE", F.col("session_no")).otherwise(0)).alias("de_session_no"),
        F.max(F.when(F.col("event_type") == "page_view", F.col("prev_session_no"))).alias("page_view_max_prev_session_no"),
    )
    .filter(F.col("distinct_event_types") > 1)
    .orderBy(F.col("prev_session_no").asc_nulls_last())
)

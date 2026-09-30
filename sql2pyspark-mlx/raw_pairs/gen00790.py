from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("event_type").orderBy("event_time", "id")
w2 = Window.partitionBy("event_type").orderBy("event_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("browser").orderBy("event_time", "id")
w4 = Window.partitionBy("browser").orderBy("event_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

base = (
    df_web_events
    .filter(F.col("event_time") > F.lit("2023-05-15 06:00:00").cast("timestamp"))
    .withColumn("years_since_event_time", (F.datediff(F.current_date(), "event_time") / 365.25))
    .withColumn("event_type_group", F.when(F.col("event_type").isin(["add_to_cart", "click"]), "Priority").otherwise("Other"))
)

with_prev = base.withColumn("prev_event_time", F.lag("event_time").over(w))

with_gaps = (
    with_prev
    .withColumn("is_new_burst", F.when(F.col("prev_event_time").isNull() | (F.col("event_time") > F.col("prev_event_time") + F.expr("INTERVAL 14 DAYS")), 1).otherwise(0))
)

islands = with_gaps.withColumn("island_id", F.sum("is_new_burst").over(w2))

with_dims = (
    islands
    .alias("a")
    .join(df_sessions.alias("d"), F.col("a.session_id") == F.col("d.id"), "left")
    .select("a.*", F.col("d.browser"), F.col("d.country"))
)

with_prev_2 = with_dims.withColumn("prev_event_time_2", F.lag("event_time").over(w3))

flagged = (
    with_prev_2
    .withColumn("session_start_flag", F.when(F.col("prev_event_time_2").isNull() | (F.col("event_time") > F.col("prev_event_time_2") + F.expr("INTERVAL 14 DAYS")), 1).otherwise(0))
)

sessions_web_events = (
    flagged
    .select(
        "id",
        "session_id",
        "event_time",
        "event_type_group",
        "island_id",
        "browser",
        F.sum("session_start_flag").over(w4).alias("burst_id"),
    )
)

kept_keys = (
    sessions_web_events
    .select("id", "session_id")
    .intersectAll(
        df_web_events
        .filter(F.col("event_time") < "2023-09-30")
        .select("id", "session_id")
    )
)

reconciled = (
    sessions_web_events
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.session_id") == F.col("k.session_id")), "inner")
    .select("s.*")
)

matched_keys = (
    reconciled
    .select("session_id", "id")
    .subtract(
        df_web_events
        .filter(F.col("duration_sec") > 284)
        .select("session_id", "id")
    )
)

kept_rows = (
    reconciled
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.session_id") == F.col("k.session_id")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

filtered = (
    kept_rows
    .filter(F.col("browser").isNotNull() & F.col("session_id").isin([7, 14, 19]))
    .select("*", F.date_trunc("YEAR", "event_time").alias("event_time_period"))
)

result = (
    filtered
    .groupBy(
        "session_id",
        F.when(F.col("burst_id") >= 6, "LARGE").otherwise("SMALL").alias("burst_id_tier"),
    )
    .agg(
        F.size(F.collect_set("browser")).alias("num_browsers"),
        F.count("*").alias("event_count"),
        F.sum("island_id").alias("total_island_id"),
        F.countDistinct("browser").alias("distinct_browsers"),
        F.sum("burst_id").alias("total_burst_id"),
    )
    .orderBy(F.col("event_count").desc_nulls_first())
)

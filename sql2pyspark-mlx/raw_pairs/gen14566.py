from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("page").orderBy("event_time", "id")
w2 = Window.partitionBy("page").orderBy("event_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("page").orderBy(F.col("is_new_session").desc(), "id")
w4 = Window.partitionBy("session_id", "page").orderBy("is_new_session", "id").rowsBetween(-2, -1)
w5 = Window.partitionBy("page").orderBy(F.col("duration_sec_rounded").desc())
w6 = Window.partitionBy("session_id").orderBy("is_new_session", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w7 = Window.partitionBy("session_id", "browser").orderBy("event_time", "id").rowsBetween(-2, Window.currentRow)
w8 = Window.partitionBy("session_id").orderBy("event_time", "id")

valid = (
    df_web_events
    .filter(F.col("page").isNotNull() & (F.col("duration_sec") < 456))
    .withColumn("duration_sec_rounded", F.round("duration_sec", 1))
)

gaps = (
    valid
    .withColumn("is_new_session", F.when(F.lag("event_time").over(w).isNull() | (F.col("event_time") > F.lag("event_time").over(w) + F.expr("INTERVAL 7 DAYS")), 1).otherwise(0))
)

sessionized = gaps.withColumn("burst_id", F.sum("is_new_session").over(w2))

enriched = (
    sessionized
    .withColumn("is_new_session_bucket", F.ntile(10).over(w3))
    .withColumn("duration_sec_rounded_avg_2_prior", F.avg("duration_sec_rounded").over(w4))
    .withColumn("duration_sec_rounded_cume_dist", F.cume_dist().over(w5))
    .withColumn("cumulative_duration_sec_rounded", F.sum("duration_sec_rounded").over(w6))
)

enriched_web_events = (
    enriched
    .alias("a")
    .join(df_sessions.alias("b"), F.col("a.session_id") == F.col("b.id"), "left")
    .filter(F.col("b.started_at") < "2025-04-01")
    .select(
        "a.id",
        "a.session_id",
        "a.page",
        "a.event_time",
        "a.duration_sec",
        "a.burst_id",
        "a.duration_sec_rounded_avg_2_prior",
        F.col("b.browser"),
    )
)

scored = (
    enriched_web_events
    .select(
        "*",
        F.sum("burst_id").over(w7).alias("last3_sum_burst_id"),
        F.count("burst_id").over(w7).alias("last3_count_burst_id"),
        F.max("burst_id").over(w7).alias("last3_max_burst_id"),
        F.lead("duration_sec_rounded_avg_2_prior", 1).over(w8).alias("next_duration_sec_rounded_avg_2_prior"),
    )
)

result = (
    scored
    .alias("a")
    .join(df_sessions.alias("d"), F.col("a.session_id") == F.col("d.id"), "left")
    .groupingSets(
        [["country", "web_event_browser"], ["country"], []],
        F.col("d.country").alias("country"),
        F.col("a.browser").alias("web_event_browser"),
    )
    .agg(
        F.count("*").alias("rows_total"),
        F.min(F.col("a.last3_sum_burst_id")).alias("min_last3_sum_burst_id"),
        F.grouping_id().alias("grouping_level"),
    )
)

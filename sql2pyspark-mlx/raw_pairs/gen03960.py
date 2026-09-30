from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.duration_sec").desc(), F.col("c.id").desc())
w2 = Window.partitionBy("page").orderBy("web_event_event_time", "id")
w3 = Window.partitionBy("page").orderBy("web_event_event_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("page").orderBy(F.col("web_event_event_time").asc_nulls_last(), "id")
w_grp = Window.partitionBy("page").orderBy("started_at", "id")
w6 = Window.orderBy(F.col("is_new_burst").desc(), "id")
w7 = Window.partitionBy("page").orderBy("is_new_burst", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w8 = Window.orderBy("cumulative_is_new_burst", "id").rowsBetween(-6, Window.currentRow)

filtered = (
    df_sessions
    .filter((F.col("started_at") > F.lit("2023-03-16").cast("date")) & F.col("browser").isin(["Safari", "Edge"]))
    .select(
        "id",
        "customer_id",
        "started_at",
        F.initcap(F.trim("device")).alias("device_title"),
        F.date_format("started_at", "yyyy-MM-dd").alias("started_at_label"),
        F.coalesce("browser", F.lit("NONE")).alias("browser_clean"),
    )
)

with_latest = (
    filtered
    .alias("s")
    .join(df_web_events.alias("c"), (F.col("c.session_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.started_at",
        "s.device_title",
        "s.started_at_label",
        "s.browser_clean",
        F.col("c.page"),
        F.col("c.event_time").alias("web_event_event_time"),
    )
)

with_gaps = (
    with_latest
    .withColumn("is_new_burst", F.when(F.lag("web_event_event_time").over(w2).isNull() | (F.col("web_event_event_time") > F.lag("web_event_event_time").over(w2) + F.expr("INTERVAL 7 DAYS")), 1).otherwise(0))
)

sessions_sessions = with_gaps.withColumn("burst_id", F.sum("is_new_burst").over(w3))

deduped = (
    sessions_sessions
    .withColumn("rank_in_group", F.row_number().over(w4))
    .filter(F.col("rank_in_group") == 1)
    .select("id", "started_at", "browser_clean", "page", "is_new_burst")
)

not_flagged = (
    deduped
    .alias("s")
    .join(df_web_events.alias("c"), (F.col("c.session_id") == F.col("s.id")) & (F.col("c.event_time") >= F.col("s.started_at")), "left_semi")
)

scored = (
    not_flagged
    .select(
        "id",
        "started_at",
        "browser_clean",
        F.lead("is_new_burst", 2).over(w_grp).alias("next_is_new_burst"),
        F.ntile(5).over(w6).alias("is_new_burst_bucket"),
        F.sum("is_new_burst").over(w7).alias("cumulative_is_new_burst"),
    )
)

result = (
    scored
    .select(
        "*",
        F.sum("cumulative_is_new_burst").over(w8).alias("last7_sum_cumulative_is_new_burst"),
        F.count("cumulative_is_new_burst").over(w8).alias("last7_count_cumulative_is_new_burst"),
        F.min("cumulative_is_new_burst").over(w8).alias("last7_min_cumulative_is_new_burst"),
    )
)

from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("page").orderBy("event_time", "id")
w2 = Window.partitionBy("page").orderBy("event_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy(F.to_date("event_time")).orderBy("duration_sec", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_recent = Window.partitionBy(F.date_trunc("MONTH", "event_time"), "page").orderBy("event_time", "id")
w_ordered = Window.orderBy("event_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_recent_2 = Window.partitionBy("page")
w7 = Window.partitionBy("page").orderBy(F.col("event_time").cast("bigint")).rangeBetween(-7776000, Window.currentRow)
w8 = Window.orderBy("session_no", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w9 = Window.orderBy("session_no", "id")
w10 = Window.orderBy("session_no", "id").rowsBetween(-6, -1)
w11 = Window.orderBy("next_session_no_avg_6_prior", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w12 = Window.orderBy("session_no")
w_running = Window.orderBy("running_duration_sec", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w14 = Window.orderBy("next_session_no_avg_6_prior", "id").rowsBetween(-3, Window.currentRow)
w15 = Window.orderBy("running_min_session_no_avg_90d", "id").rowsBetween(-6, -1)

base = (
    df_web_events
    .filter((F.col("event_time") >= F.lit("2023-04-15").cast("date")) & F.col("page").isNotNull())
    .select(
        "id",
        "session_id",
        "event_type",
        "page",
        "event_time",
        "duration_sec",
        F.coalesce("duration_sec", F.lit(0)).alias("duration_sec_filled"),
        F.coalesce("page", F.lit("NONE")).alias("resolved_page"),
    )
)

gaps = (
    base
    .withColumn("new_group_flag", F.when(F.col("event_time") - F.lag("event_time").over(w) > F.expr("INTERVAL 30 DAYS"), 1).otherwise(0))
)

sessionized = (
    gaps
    .select(
        "id",
        "page",
        "event_time",
        "duration_sec",
        F.sum("new_group_flag").over(w2).alias("session_no"),
    )
)

metrics = (
    sessionized
    .select(
        "id",
        "session_no",
        F.sum("duration_sec").over(w3).alias("running_duration_sec"),
        F.lead("session_no", 2).over(w_recent).alias("next_session_no"),
        F.sum("session_no").over(w_ordered).alias("running_session_no"),
        F.min("duration_sec").over(w_recent_2).alias("page_min_duration_sec"),
        F.avg("session_no").over(w7).alias("session_no_avg_90d"),
    )
)

metrics_web_events = (
    metrics
    .withColumn("first_known_page_min_duration_sec", F.first("page_min_duration_sec", ignorenulls=True).over(w8))
    .withColumn("next_session_no_avg_90d", F.lead("session_no_avg_90d", 1).over(w9))
    .withColumn("next_session_no_avg_6_prior", F.avg("next_session_no").over(w10))
)

scored = (
    metrics_web_events
    .select(
        "id",
        "session_no",
        "running_duration_sec",
        "running_session_no",
        "next_session_no_avg_6_prior",
        F.count("session_no_avg_90d").over(w11).alias("running_count_session_no_avg_90d"),
        F.min("session_no_avg_90d").over(w11).alias("running_min_session_no_avg_90d"),
        F.max("session_no_avg_90d").over(w11).alias("running_max_session_no_avg_90d"),
    )
)

metrics_web_events_2 = (
    scored
    .withColumn("session_no_percent_rank", F.percent_rank().over(w12))
    .withColumn("last_known_running_count_session_no_avg_90d", F.last("running_count_session_no_avg_90d", ignorenulls=True).over(w11))
    .withColumn("cumulative_running_max_session_no_avg_90d", F.sum("running_max_session_no_avg_90d").over(w_running))
)

enriched = (
    metrics_web_events_2
    .select(
        "id",
        "running_duration_sec",
        "running_session_no",
        "next_session_no_avg_6_prior",
        "running_count_session_no_avg_90d",
        "running_min_session_no_avg_90d",
        "running_max_session_no_avg_90d",
        "session_no_percent_rank",
        "last_known_running_count_session_no_avg_90d",
        F.avg("running_session_no").over(w14).alias("last4_avg_running_session_no"),
        F.count("running_session_no").over(w14).alias("last4_count_running_session_no"),
        F.sum("running_session_no").over(w14).alias("last4_sum_running_session_no"),
        F.min("running_session_no").over(w14).alias("last4_min_running_session_no"),
        F.avg("session_no").over(w15).alias("session_no_avg_6_prior"),
    )
)

result = (
    enriched
    .select(
        "id",
        "running_max_session_no_avg_90d",
        "session_no_percent_rank",
        "last4_avg_running_session_no",
    )
    .unpivot(
        ids=["id"],
        values=["running_max_session_no_avg_90d", "session_no_percent_rank", "last4_avg_running_session_no"],
        variableColumnName="kpi",
        valueColumnName="val",
    )
    .filter(F.col("val").isNotNull())
)

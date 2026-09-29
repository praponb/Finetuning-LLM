from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("session_id").orderBy("event_time_period", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("session_id").orderBy("months_since_event_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.orderBy("first_known_months_since_event_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

cleaned = (
    df_web_events
    .filter(F.col("page").isNotNull())
    .select(
        "id",
        "session_id",
        "event_time",
        "duration_sec",
        F.when(F.col("duration_sec") >= 511, "High").when(F.col("duration_sec") >= 123, "Medium").otherwise("Low").alias("duration_sec_tier"),
        F.initcap(F.trim("event_type")).alias("event_type_title"),
    )
)

base = (
    cleaned
    .filter(F.to_date("event_time") < "2023-12-01")
    .select(
        "id",
        "session_id",
        "duration_sec_tier",
        (F.datediff(F.current_date(), "event_time") / 30).alias("months_since_event_time"),
        F.date_trunc("YEAR", "event_time").alias("event_time_period"),
    )
)

scored = (
    base
    .select(
        "id",
        "duration_sec_tier",
        "event_time_period",
        F.first("months_since_event_time", ignorenulls=True).over(w).alias("first_known_months_since_event_time"),
        F.last("months_since_event_time", ignorenulls=True).over(w2).alias("last_known_months_since_event_time"),
    )
)

scored_web_events = (
    scored
    .select(
        "id",
        "duration_sec_tier",
        "event_time_period",
        "last_known_months_since_event_time",
        F.max("last_known_months_since_event_time").over(w3).alias("running_max_last_known_months_since_event_time"),
        F.sum("last_known_months_since_event_time").over(w3).alias("running_sum_last_known_months_since_event_time"),
        F.avg("last_known_months_since_event_time").over(w3).alias("running_avg_last_known_months_since_event_time"),
        F.count("last_known_months_since_event_time").over(w3).alias("running_count_last_known_months_since_event_time"),
    )
)

result = (
    scored_web_events
    .select(
        "id",
        "event_time_period",
        "last_known_months_since_event_time",
        "running_max_last_known_months_since_event_time",
        "running_sum_last_known_months_since_event_time",
        "running_count_last_known_months_since_event_time",
    )
    .orderBy("running_max_last_known_months_since_event_time")
)

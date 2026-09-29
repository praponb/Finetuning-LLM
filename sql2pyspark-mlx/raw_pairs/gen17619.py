from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("session_id").orderBy("event_time", "id").rowsBetween(-2, -1)
w_running = Window.partitionBy("session_id").orderBy("duration_sec", "id")
w3 = Window.orderBy("duration_sec", "id").rowsBetween(-6, -1)
w4 = Window.partitionBy(F.year("event_time"), "page")
w_recent = Window.partitionBy("event_type").orderBy("event_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

filtered = (
    df_web_events
    .filter((F.col("event_time") > F.lit("2023-08-29").cast("date")) & F.col("page").isNotNull())
    .select(
        "*",
        F.concat_ws("-", "page", "event_type").alias("page_event_type_key"),
        F.coalesce("page", F.lit("N/A")).alias("page_clean"),
    )
)

result = (
    filtered
    .filter(F.col("duration_sec") <= 344)
    .select(
        "id",
        "page",
        "event_time",
        "duration_sec",
        "page_event_type_key",
        "page_clean",
        F.avg("duration_sec").over(w).alias("duration_sec_avg_2_prior"),
        F.lag("duration_sec", 2).over(w_running).alias("prev_duration_sec"),
        F.avg("duration_sec").over(w3).alias("duration_sec_avg_6_prior"),
        F.avg("duration_sec").over(w4).alias("event_time_year_avg_duration_sec"),
        F.first("duration_sec", ignorenulls=True).over(w_recent).alias("first_known_duration_sec"),
    )
)

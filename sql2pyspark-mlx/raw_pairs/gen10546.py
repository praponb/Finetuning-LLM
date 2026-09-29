from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("page", "session_id").orderBy("duration_sec", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("session_id").orderBy("event_time", "id").rowsBetween(-6, -1)

cleaned = (
    df_web_events
    .filter((F.col("event_time") > F.to_timestamp(F.lit("2023-07-01 05:00:00"))) & F.col("page").isNotNull())
    .select(
        "*",
        F.lower("event_type").alias("event_type_lower"),
        F.concat("event_type", F.lit(" "), "page").alias("event_type_page"),
        F.coalesce("page", F.lit("DIRECT")).alias("resolved_page"),
    )
)

result = (
    cleaned
    .select(
        "id",
        "session_id",
        "event_type",
        "page",
        "event_time",
        "duration_sec",
        "event_type_page",
        F.max("duration_sec").over(w).alias("running_max_duration_sec"),
        F.min("duration_sec").over(w).alias("running_min_duration_sec"),
        F.avg("duration_sec").over(w).alias("running_avg_duration_sec"),
        F.avg("duration_sec").over(w2).alias("duration_sec_avg_6_prior"),
    )
)

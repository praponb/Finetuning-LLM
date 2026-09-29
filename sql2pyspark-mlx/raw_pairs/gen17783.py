from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("page")
w2 = Window.partitionBy("event_type").orderBy(F.col("years_since_event_time").asc_nulls_last())

cleaned = (
    df_web_events
    .filter((F.col("event_time") >= F.lit("2023-03-22 05:00:00").cast("timestamp")) & F.col("page").isNotNull())
    .select(
        "*",
        (F.datediff(F.current_date(), "event_time") / 365).alias("years_since_event_time"),
    )
)

result = (
    cleaned
    .select(
        "id",
        "session_id",
        "event_type",
        "event_time",
        "duration_sec",
        "years_since_event_time",
        F.stddev_samp("years_since_event_time").over(w).alias("page_stddev_samp_years_since_event_time"),
        F.dense_rank().over(w2).alias("years_since_event_time_dense_rank"),
    )
)

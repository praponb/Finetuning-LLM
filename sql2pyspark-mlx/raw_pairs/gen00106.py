from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.date_trunc("month", "event_time"), "page").orderBy("event_time", "id")
w2 = Window.partitionBy(F.month("event_time"), "page").orderBy(F.col("duration_sec").desc(), "id")
w3 = Window.partitionBy("page")
w4 = Window.orderBy(F.col("event_time").cast("bigint")).rangeBetween(-604800, Window.currentRow)

filtered = (
    df_web_events
    .filter((F.col("event_time") >= F.to_timestamp(F.lit("2023-05-03 02:00:00"))) & ~F.col("event_type").isin(["search", "page_view"]))
    .select(
        "id",
        "page",
        "event_time",
        "duration_sec",
        (F.datediff(F.current_date(), "event_time") / 365.25).alias("years_since_event_time"),
    )
)

result = (
    filtered
    .filter(F.col("event_time") >= "2025-01-24")
    .select(
        "id",
        "page",
        F.row_number().over(w).alias("seq_no"),
        F.row_number().over(w2).alias("seq_no_2"),
        F.stddev_samp("years_since_event_time").over(w3).alias("page_stddev_samp_years_since_event_time"),
        F.count("years_since_event_time").over(w4).alias("years_since_event_time_count_7d"),
    )
)

from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("page").orderBy("duration_sec", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("session_id").orderBy("duration_sec", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("event_type").orderBy("event_time", "id")
w4 = Window.partitionBy(F.to_date("event_time"), "session_id").orderBy("cumulative_duration_sec", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy(F.date_trunc("YEAR", "event_time"), "session_id").orderBy("cumulative_duration_sec", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.partitionBy(F.year("event_time")).orderBy("event_time", "id").rowsBetween(-5, -1)
w7 = Window.partitionBy("session_id").orderBy(F.col("event_time").cast("bigint")).rangeBetween(-604800, Window.currentRow)
w8 = Window.partitionBy("event_type")
w9 = Window.partitionBy("session_id", "event_type").orderBy("cumulative_cumulative_duration_sec", "id")
w10 = Window.partitionBy("session_id").orderBy(F.col("event_time").desc(), "id")
w11 = Window.partitionBy("session_id", "event_type").orderBy(F.col("event_time").cast("bigint")).rangeBetween(-604800, Window.currentRow)
w12 = Window.partitionBy("event_type").orderBy("cumulative_cumulative_duration_sec", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

cleaned = (
    df_web_events
    .filter((F.col("event_time") >= F.lit("2023-09-02").cast("date")) & F.col("page").isNotNull())
    .withColumn("event_type_prefix", F.substring("event_type", 1, 4))
    .withColumn("page_upper", F.upper("page"))
)

with_windows = (
    cleaned
    .select(
        "id",
        "session_id",
        "event_type",
        "event_time",
        "page_upper",
        F.sum("duration_sec").over(w).alias("cumulative_duration_sec"),
        F.last("page", ignorenulls=True).over(w2).alias("last_known_page"),
    )
)

with_windows_web_events = (
    with_windows
    .filter(F.col("event_type").isin(["page_view", "search"]))
    .select(
        "*",
        F.row_number().over(w3).alias("row_seq"),
        F.sum("cumulative_duration_sec").over(w4).alias("cumulative_cumulative_duration_sec"),
        F.sum("cumulative_duration_sec").over(w5).alias("running_cumulative_duration_sec"),
        F.avg("cumulative_duration_sec").over(w6).alias("cumulative_duration_sec_avg_5_prior"),
        F.avg("cumulative_duration_sec").over(w7).alias("cumulative_duration_sec_avg_7d"),
    )
)

metrics = (
    with_windows_web_events
    .select(
        "id",
        "session_id",
        "event_type",
        "event_time",
        "cumulative_duration_sec",
        "last_known_page",
        "running_cumulative_duration_sec",
        "cumulative_duration_sec_avg_5_prior",
        F.round((F.col("row_seq") / F.sum("row_seq").over(w8)), 4).alias("row_seq_share"),
        F.row_number().over(w9).alias("event_seq"),
        F.row_number().over(w10).alias("row_seq_2"),
        F.count("cumulative_duration_sec").over(w11).alias("cumulative_duration_sec_count_7d"),
        F.sum("running_cumulative_duration_sec").over(w12).alias("cumulative_running_cumulative_duration_sec"),
    )
)

result = (
    metrics
    .groupBy(
        F.when(F.col("cumulative_duration_sec_avg_5_prior") >= 2221, "HIGH").otherwise("LOW").alias("cumulative_duration_sec_avg_5_prior_tier"),
    )
    .agg(
        F.sum(F.when(F.col("event_type") == "click", F.col("cumulative_duration_sec")).otherwise(0)).alias("click_cumulative_duration_sec"),
        F.count(F.when(F.col("event_type") == "search", 1)).alias("search_rows"),
        F.round(F.avg("row_seq_share"), 2).alias("avg_row_seq_share"),
        F.round(F.avg("row_seq_share"), 2).alias("avg_row_seq_share_2"),
        F.max("event_time").alias("ended_at"),
    )
    .orderBy("search_rows")
)

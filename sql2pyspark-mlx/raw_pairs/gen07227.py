from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("event_time", "id")
w2 = Window.orderBy(F.col("row_seq").asc_nulls_last())
w3 = Window.orderBy(F.col("duration_sec_filled").desc(), "id")
w4 = Window.orderBy("duration_sec_filled_scaled", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.orderBy("duration_sec_filled_times_row_seq", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.orderBy(F.col("prev_duration_sec_filled").desc(), "id")
w7 = Window.orderBy("event_seq", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w8 = Window.orderBy("row_seq_2", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w9 = Window.orderBy(F.col("duration_sec_filled_times_row_seq").desc(), "id")
w10 = Window.orderBy("duration_sec_filled", "id").rowsBetween(-6, Window.currentRow)

base = (
    df_web_events
    .filter(F.col("duration_sec") >= 251)
    .select(
        "id",
        "event_time",
        F.concat_ws("/", "event_type", "page").alias("event_type_page_key"),
        F.coalesce("duration_sec", F.lit(0)).alias("duration_sec_filled"),
    )
)

enriched = (
    base
    .select(
        "*",
        F.lag("duration_sec_filled").over(w).alias("prev_duration_sec_filled"),
        F.row_number().over(w).alias("row_seq"),
    )
)

valid = (
    enriched
    .filter(F.col("row_seq") > 4)
    .withColumn("duration_sec_filled_scaled", F.round((F.col("duration_sec_filled") * 1.15), 2))
)

filtered = (
    valid
    .filter(F.col("event_time") >= F.to_date(F.lit("2023-08-27")))
    .select(
        "*",
        (F.col("duration_sec_filled") * F.col("row_seq")).alias("duration_sec_filled_times_row_seq"),
    )
)

enriched_web_events = (
    filtered
    .select(
        "id",
        "event_type_page_key",
        "duration_sec_filled",
        "duration_sec_filled_scaled",
        "duration_sec_filled_times_row_seq",
        F.percent_rank().over(w2).alias("row_seq_percent_rank"),
        F.row_number().over(w3).alias("row_seq_2"),
        F.first("duration_sec_filled", ignorenulls=True).over(w4).alias("first_known_duration_sec_filled"),
        F.first("duration_sec_filled_times_row_seq", ignorenulls=True).over(w5).alias("first_known_duration_sec_filled_times_row_seq"),
        F.row_number().over(w6).alias("event_seq"),
    )
)

with_windows = (
    enriched_web_events
    .withColumn("running_event_seq", F.sum("event_seq").over(w7))
    .withColumn("running_row_seq_2", F.sum("row_seq_2").over(w8))
    .withColumn("duration_sec_filled_times_row_seq_bucket", F.ntile(5).over(w9))
)

metrics = (
    with_windows
    .withColumn("duration_sec_filled_scaled_avg_6", F.avg("duration_sec_filled_scaled").over(w10))
    .withColumn("cumulative_duration_sec_filled", F.sum("duration_sec_filled").over(w8))
)

result = (
    metrics
    .groupBy(
        F.when(F.col("duration_sec_filled_scaled") >= 74, "LARGE").otherwise("SMALL").alias("duration_sec_filled_scaled_tier"),
    )
    .agg(
        F.percentile_approx("running_event_seq", 0.9).alias("running_event_seq_p90"),
        F.percentile_approx("duration_sec_filled_times_row_seq", [0.25, 0.5, 0.75]).alias("duration_sec_filled_times_row_seq_quartiles"),
        F.percentile_approx("running_event_seq", [0.25, 0.5, 0.75]).alias("running_event_seq_quartiles"),
        F.count("*").alias("row_count"),
        F.percentile_approx("cumulative_duration_sec_filled", 0.95).alias("cumulative_duration_sec_filled_p95"),
        F.round(F.avg("event_seq"), 2).alias("avg_event_seq"),
    )
)

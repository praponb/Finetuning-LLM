from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("page", "country").orderBy("duration_sec")
w2 = Window.partitionBy(F.date_trunc("WEEK", "event_time"), "country").orderBy("duration_sec", "id")
w3 = Window.partitionBy("country", "event_type").orderBy("event_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

with_dims = (
    df_web_events
    .alias("e")
    .join(df_sessions.alias("d"), F.col("e.session_id") == F.col("d.id"), "left")
    .filter(F.col("d.browser") == "Firefox")
    .select(
        "e.id",
        "e.session_id",
        "e.event_type",
        "e.page",
        "e.event_time",
        "e.duration_sec",
        F.col("d.country"),
        F.col("d.device"),
    )
)

deduped = (
    with_dims
    .withColumn("dedup_rank", F.rank().over(w))
    .filter(F.col("dedup_rank") == 1)
    .drop("dedup_rank")
)

scored = (
    deduped
    .withColumn("row_seq", F.row_number().over(w2))
    .withColumn("cumulative_duration_sec", F.sum("duration_sec").over(w3))
)

valid = (
    scored
    .filter(F.col("country").isNotNull() & F.col("row_seq").between(5, 6))
    .select(
        "id",
        "event_time",
        "duration_sec",
        "country",
        "row_seq",
        "cumulative_duration_sec",
        F.date_trunc("month", "event_time").alias("event_time_period"),
        F.round((F.col("duration_sec") / F.col("row_seq")), 2).alias("duration_sec_per_row_seq"),
        F.coalesce("country", F.lit("N/A")).alias("country_filled"),
    )
)

result = (
    valid
    .groupBy(
        F.when(F.col("duration_sec") >= 483, "PREMIUM").otherwise("STANDARD").alias("duration_sec_tier"),
        "country",
    )
    .agg(
        F.count("*").alias("num_rows"),
        F.percentile_approx("duration_sec_per_row_seq", 0.95).alias("duration_sec_per_row_seq_p95"),
        F.avg(F.when(F.col("country") == "SG", F.col("cumulative_duration_sec"))).alias("sg_avg_cumulative_duration_sec"),
        F.sort_array(F.collect_set("country")).alias("country_list"),
    )
)

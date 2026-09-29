from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_part = Window.partitionBy("page", "session_id").orderBy("duration_sec_adjusted", "id")
w2 = Window.partitionBy("session_id").orderBy(F.col("prev_duration_sec_adjusted").desc(), "id")
w_ordered = Window.orderBy("next_duration_sec", "id")
w4 = Window.partitionBy(F.to_date("event_time"), "device").orderBy("seq_no", "id")
w5 = Window.partitionBy(F.year("event_time"), "page")
w6 = Window.partitionBy("country").orderBy(F.col("event_time").desc(), "id")
w7 = Window.partitionBy("device", "browser").orderBy("next_duration_sec", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w8 = Window.partitionBy("browser").orderBy("duration_sec", "id")
w9 = Window.partitionBy("browser").orderBy("seq_no", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_grp = Window.partitionBy("device").orderBy("seq_no", "id")
w11 = Window.partitionBy("device").orderBy(F.col("next_seq_no_3").asc_nulls_last())

cleaned = (
    df_web_events
    .filter(F.col("event_time") > F.to_timestamp(F.lit("2023-05-25 02:00:00")))
    .withColumn("duration_sec_adjusted", F.round((F.col("duration_sec") * 1.07), 2))
    .withColumn("resolved_page", F.coalesce("page", F.lit("UNKNOWN")))
)

metrics = (
    cleaned
    .withColumn("prev_duration_sec_adjusted", F.lag("duration_sec_adjusted").over(w_part))
    .withColumn("next_duration_sec", F.lead("duration_sec").over(w_part))
    .withColumn("seq_no", F.row_number().over(w_part))
)

with_dims = (
    metrics
    .alias("s")
    .join(df_sessions.alias("p2"), F.col("s.session_id") == F.col("p2.id"), "left")
    .filter(F.col("p2.customer_id").isin([12, 17]))
    .select("s.*", F.col("p2.browser"), F.col("p2.country"), F.col("p2.device"))
)

with_windows = (
    with_dims
    .select(
        "id",
        "duration_sec",
        "duration_sec_adjusted",
        "resolved_page",
        "next_duration_sec",
        "seq_no",
        "browser",
        "device",
        F.row_number().over(w2).alias("row_seq"),
        F.row_number().over(w_ordered).alias("seq_no_2"),
        F.row_number().over(w4).alias("row_seq_2"),
        F.stddev_samp("duration_sec_adjusted").over(w5).alias("event_time_year_stddev_samp_duration_sec_adjusted"),
        F.row_number().over(w6).alias("seq_no_3"),
    )
)

enriched = (
    with_windows
    .select(
        "id",
        "resolved_page",
        "next_duration_sec",
        "seq_no",
        "device",
        "row_seq",
        "row_seq_2",
        "event_time_year_stddev_samp_duration_sec_adjusted",
        F.first("device", ignorenulls=True).over(w7).alias("first_known_device"),
        F.lead("seq_no_3", 2, 0).over(w8).alias("next_seq_no_3"),
        F.sum("duration_sec").over(w9).alias("cumulative_duration_sec"),
    )
)

base = (
    enriched
    .filter(F.col("device") != "desktop")
    .select(
        "*",
        F.col("event_time_year_stddev_samp_duration_sec_adjusted").cast("string").alias("event_time_year_stddev_samp_duration_sec_adjusted_num"),
    )
)

result = (
    base
    .select(
        "id",
        "next_duration_sec",
        "event_time_year_stddev_samp_duration_sec_adjusted",
        "next_seq_no_3",
        "event_time_year_stddev_samp_duration_sec_adjusted_num",
        F.lead("event_time_year_stddev_samp_duration_sec_adjusted").over(w_grp).alias("next_event_time_year_stddev_samp_duration_sec_adjusted"),
        F.lag("event_time_year_stddev_samp_duration_sec_adjusted").over(w_grp).alias("prev_event_time_year_stddev_samp_duration_sec_adjusted"),
        F.lead("cumulative_duration_sec").over(w_grp).alias("next_cumulative_duration_sec"),
        F.row_number().over(w_grp).alias("seq_no_2"),
        F.percent_rank().over(w11).alias("next_seq_no_3_percent_rank"),
    )
)

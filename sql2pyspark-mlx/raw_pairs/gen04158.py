from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("duration_sec", "id").rowsBetween(-3, Window.currentRow)
w2 = Window.partitionBy(F.year("event_time")).orderBy("event_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_grp = Window.orderBy("last4_avg_duration_sec", "id")
w4 = Window.orderBy("first_known_duration_sec_str", "id").rowsBetween(-5, -1)
w5 = Window.orderBy("last4_sum_duration_sec", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.orderBy("first_known_duration_sec", "id")
w7 = Window.partitionBy(F.date_trunc("month", "event_time_period")).orderBy("prev_duration_sec_whole", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

valid = (
    df_web_events
    .filter(F.col("page").isNotNull() & (F.col("page").eqNullSafe(F.lit("/home"))))
    .select(
        "id",
        "event_time",
        "duration_sec",
        F.concat_ws("/", "event_type", "page").alias("event_type_page_key"),
        F.date_trunc("YEAR", "event_time").alias("event_time_period"),
        F.coalesce("page", F.lit("UNKNOWN")).alias("page_clean"),
    )
)

metrics = (
    valid
    .select(
        "id",
        "duration_sec",
        "event_type_page_key",
        "event_time_period",
        F.avg("duration_sec").over(w).alias("last4_avg_duration_sec"),
        F.max("duration_sec").over(w).alias("last4_max_duration_sec"),
        F.sum("duration_sec").over(w).alias("last4_sum_duration_sec"),
        F.first("duration_sec", ignorenulls=True).over(w2).alias("first_known_duration_sec"),
    )
)

filtered = (
    metrics
    .filter(F.col("last4_max_duration_sec") >= 460)
    .withColumn("first_known_duration_sec_str", F.col("first_known_duration_sec").cast("double"))
    .withColumn("duration_sec_whole", F.ceil("duration_sec"))
)

metrics_web_events = (
    filtered
    .select(
        "*",
        F.lag("last4_max_duration_sec").over(w_grp).alias("prev_last4_max_duration_sec"),
        F.lag("duration_sec_whole").over(w_grp).alias("prev_duration_sec_whole"),
        F.row_number().over(w_grp).alias("event_seq"),
        F.avg("first_known_duration_sec").over(w4).alias("first_known_duration_sec_avg_5_prior"),
    )
)

metrics_web_events_2 = (
    metrics_web_events
    .withColumn("last_known_first_known_duration_sec", F.last("first_known_duration_sec", ignorenulls=True).over(w5))
    .withColumn("first_known_duration_sec_bucket", F.ntile(5).over(w6))
    .withColumn("cumulative_duration_sec_whole", F.sum("duration_sec_whole").over(w7))
)

valid_web_events = (
    metrics_web_events_2
    .filter(F.col("last4_avg_duration_sec") < 480)
    .select(
        "id",
        "duration_sec",
        "event_time_period",
        "last4_avg_duration_sec",
        "last4_max_duration_sec",
        "last4_sum_duration_sec",
        "first_known_duration_sec_str",
        "duration_sec_whole",
        "prev_duration_sec_whole",
        "first_known_duration_sec_avg_5_prior",
        "last_known_first_known_duration_sec",
        "first_known_duration_sec_bucket",
        "cumulative_duration_sec_whole",
        F.round((F.col("duration_sec") * 0.85), 2).alias("duration_sec_adjusted"),
    )
)

result = (
    valid_web_events
    .cube(
        F.when(F.col("last4_max_duration_sec") >= 440, "LARGE").otherwise("SMALL").alias("last4_max_duration_sec_tier"),
    )
    .agg(
        F.bit_or("duration_sec").alias("duration_sec_bit_or"),
        F.percentile_approx("last_known_first_known_duration_sec", [0.25, 0.5, 0.75]).alias("last_known_first_known_duration_sec_quartiles"),
    )
)

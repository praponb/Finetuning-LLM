from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code").orderBy("reading_ts", "id")
w2 = Window.partitionBy("device_code").orderBy("reading_ts", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_part = Window.partitionBy("device_code").orderBy("session_no", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

filtered = (
    df_device_readings
    .filter(F.col("reading_ts") >= F.lit("2025-09-01").cast("date"))
    .select(
        "*",
        (F.datediff(F.current_date(), "reading_ts") / 30).alias("months_since_reading_ts"),
    )
)

prev_events = filtered.withColumn("prev_reading_ts", F.lag("reading_ts").over(w))

with_gaps = (
    prev_events
    .withColumn("is_new_burst", F.when(F.col("prev_reading_ts").isNull() | (F.col("reading_ts") > F.col("prev_reading_ts") + F.expr("INTERVAL 45 MINUTES")), 1).otherwise(0))
)

sessionized = (
    with_gaps
    .select(
        "id",
        "device_code",
        "reading_ts",
        "temp_c",
        "pressure",
        "firmware",
        F.sum("is_new_burst").over(w2).alias("session_no"),
    )
)

with_gaps_2 = (
    sessionized
    .withColumn("is_new_session", F.when(F.col("reading_ts") - F.lag("reading_ts").over(w) > F.expr("INTERVAL 20 MINUTES"), 1).otherwise(0))
)

sessionized_device_readings = with_gaps_2.withColumn("island_id", F.sum("is_new_session").over(w2))

with_windows = (
    sessionized_device_readings
    .filter(F.col("firmware") == "1.1.0")
    .select(
        "id",
        "device_code",
        "reading_ts",
        "temp_c",
        "session_no",
        "is_new_session",
        "island_id",
        F.max("temp_c").over(w_part).alias("running_max_temp_c"),
        F.min("temp_c").over(w_part).alias("running_min_temp_c"),
        F.count("temp_c").over(w_part).alias("running_count_temp_c"),
        F.sum("temp_c").over(w_part).alias("running_sum_temp_c"),
    )
)

base = (
    with_windows
    .filter(F.col("running_max_temp_c").between(31, 36))
    .select(
        "id",
        "is_new_session",
        "running_min_temp_c",
        (F.col("session_no") - F.col("island_id")).alias("session_no_minus_island_id"),
        F.ceil("session_no").alias("session_no_whole"),
    )
)

valid = (
    base
    .filter(F.col("is_new_session") != 0)
    .withColumn("is_new_session_rounded", F.round("is_new_session", 2))
    .withColumn("session_no_whole_minus_running_min_temp_c", (F.col("session_no_whole") - F.col("running_min_temp_c")))
)

result = (
    valid
    .groupBy(
        F.when(F.col("session_no_whole") >= 87, "HIGH").otherwise("LOW").alias("session_no_whole_tier"),
        F.when(F.col("running_min_temp_c") >= 38.2, "LARGE").otherwise("SMALL").alias("running_min_temp_c_tier"),
        F.when(F.col("is_new_session_rounded") >= 73, "PREMIUM").otherwise("STANDARD").alias("is_new_session_rounded_tier"),
    )
    .agg(
        F.percentile_approx("is_new_session_rounded", [0.25, 0.5, 0.75]).alias("is_new_session_rounded_quartiles"),
        F.max("is_new_session").alias("max_is_new_session"),
        F.min("running_min_temp_c").alias("min_running_min_temp_c"),
        F.count("*").alias("num_rows"),
    )
)

from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code").orderBy("reading_ts", "id")
w2 = Window.partitionBy("device_code").orderBy("reading_ts", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("device_code").orderBy(F.col("reading_ts").desc(), "id")
w_hist = Window.orderBy("temp_c", "id")

cleaned = (
    df_device_readings
    .filter(F.col("reading_ts") >= F.lit("2025-09-01").cast("date"))
    .select(
        "*",
        (F.datediff(F.current_date(), "reading_ts") / 7).alias("weeks_since_reading_ts"),
        (F.col("pressure") * F.col("humidity")).alias("pressure_times_humidity"),
    )
)

filtered = (
    cleaned
    .filter((F.col("reading_ts") >= F.lit("2025-09-01 06:00:00").cast("timestamp")) & (F.col("pressure") <= 997))
    .withColumn("weeks_since_reading_ts_per_pressure_times_humidity", (F.col("weeks_since_reading_ts") / F.nullif("pressure_times_humidity", F.lit(0))))
    .withColumn("clean_device_code", F.trim("device_code"))
)

flagged = (
    filtered
    .withColumn("new_group_flag", F.when(F.lag("reading_ts").over(w).isNull() | (F.col("reading_ts") > F.lag("reading_ts").over(w) + F.expr("INTERVAL 15 MINUTES")), 1).otherwise(0))
)

sessionized = (
    flagged
    .select(
        "id",
        "device_code",
        "reading_ts",
        "temp_c",
        "firmware",
        "weeks_since_reading_ts",
        "clean_device_code",
        F.sum("new_group_flag").over(w2).alias("burst_id"),
    )
)

ranked = (
    sessionized
    .withColumn("row_num", F.row_number().over(w3))
    .filter(F.col("row_num") == 1)
    .select("id", "temp_c", "clean_device_code", "burst_id")
)

scored = (
    ranked
    .select(
        "id",
        "clean_device_code",
        F.lead("burst_id").over(w_hist).alias("next_burst_id"),
        F.lag("temp_c").over(w_hist).alias("prev_temp_c"),
        F.lead("temp_c").over(w_hist).alias("next_temp_c"),
        F.row_number().over(w_hist).alias("event_seq"),
    )
)

result = (
    scored
    .select(
        "id",
        "clean_device_code",
        "next_burst_id",
        "prev_temp_c",
        "next_temp_c",
        "event_seq",
    )
    .orderBy("next_burst_id")
)

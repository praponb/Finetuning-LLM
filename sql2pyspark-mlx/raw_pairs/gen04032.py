from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code").orderBy("reading_ts", "id")
w2 = Window.partitionBy("device_code").orderBy("reading_ts", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("device_code").orderBy("status_bits", "id").rowsBetween(-2, Window.currentRow)
w4 = Window.partitionBy(F.date_trunc("week", "reading_ts")).orderBy("island_id", "id")
w5 = Window.partitionBy("device_code").orderBy("session_start_flag", "id").rowsBetween(-2, -1)
w6 = Window.partitionBy(F.to_date("reading_ts")).orderBy("last3_min_session_no", "id")
w7 = Window.orderBy("last3_min_session_no", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

base = (
    df_device_readings
    .filter(F.col("status_bits") >= 5)
    .select(
        "id",
        "device_code",
        "reading_ts",
        "humidity",
        "pressure",
        "status_bits",
        "firmware",
        F.round("status_bits", 1).alias("status_bits_rounded"),
    )
)

with_gaps = (
    base
    .withColumn("new_group_flag", F.when(F.lag("reading_ts").over(w).isNull() | (F.col("reading_ts") > F.lag("reading_ts").over(w) + F.expr("INTERVAL 15 MINUTES")), 1).otherwise(0))
)

sessions_device_readings = (
    with_gaps
    .select(
        "id",
        "device_code",
        "reading_ts",
        "status_bits",
        "status_bits_rounded",
        F.sum("new_group_flag").over(w2).alias("island_id"),
    )
)

with_gaps_2 = (
    sessions_device_readings
    .withColumn("session_start_flag", F.when(F.col("reading_ts") - F.lag("reading_ts").over(w) > F.expr("INTERVAL 1 HOUR"), 1).otherwise(0))
)

sessions_device_readings_2 = with_gaps_2.withColumn("session_no", F.sum("session_start_flag").over(w2))

scored = (
    sessions_device_readings_2
    .filter(F.col("status_bits") != 11)
    .select(
        "id",
        "device_code",
        "reading_ts",
        "status_bits_rounded",
        "session_start_flag",
        F.avg("session_no").over(w3).alias("last3_avg_session_no"),
        F.min("session_no").over(w3).alias("last3_min_session_no"),
        F.max("session_no").over(w3).alias("last3_max_session_no"),
        F.sum("session_no").over(w3).alias("last3_sum_session_no"),
        F.lag("status_bits_rounded", 1).over(w4).alias("prev_status_bits_rounded"),
    )
)

result = (
    scored
    .select(
        "*",
        F.avg("prev_status_bits_rounded").over(w5).alias("prev_status_bits_rounded_avg_2_prior"),
        F.lead("last3_min_session_no", 1).over(w6).alias("next_last3_min_session_no"),
        F.sum("last3_max_session_no").over(w7).alias("cumulative_last3_max_session_no"),
    )
)

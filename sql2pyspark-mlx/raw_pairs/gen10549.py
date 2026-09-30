from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("status_bits", "id").rowsBetween(-6, Window.currentRow)
w2 = Window.partitionBy("device_code")
w3 = Window.orderBy("pressure_plus_status_bits", "id")
w4 = Window.partitionBy("device_code").orderBy(F.col("reading_ts").cast("bigint")).rangeBetween(-7200, Window.currentRow)
w5 = Window.orderBy("device_code_stddev_samp_status_bits", "id")
w6 = Window.orderBy(F.col("status_bits_rounded").desc(), "id")
w7 = Window.orderBy(F.col("status_bits_sum_2h").desc())

valid = (
    df_device_readings
    .filter(F.col("pressure") >= 995)
    .select(
        "id",
        "device_code",
        "reading_ts",
        "humidity",
        "pressure",
        "status_bits",
        "firmware",
        F.round("status_bits", 0).alias("status_bits_rounded"),
        (F.col("pressure") + F.col("status_bits")).alias("pressure_plus_status_bits"),
    )
)

scored = (
    valid
    .filter(F.col("status_bits_rounded") > 67)
    .select(
        "id",
        "reading_ts",
        "status_bits_rounded",
        F.avg("pressure").over(w).alias("pressure_avg_6"),
        F.stddev_samp("pressure").over(w2).alias("device_code_stddev_samp_pressure"),
        F.stddev_samp("status_bits").over(w2).alias("device_code_stddev_samp_status_bits"),
        F.lead("status_bits", 1, 0).over(w3).alias("next_status_bits"),
        F.sum("status_bits").over(w4).alias("status_bits_sum_2h"),
    )
)

filtered = (
    scored
    .filter((F.col("reading_ts") > F.to_timestamp(F.lit("2025-09-01 05:00:00"))) & (F.col("device_code_stddev_samp_pressure") >= 995))
    .select(
        "id",
        "reading_ts",
        "status_bits_rounded",
        "pressure_avg_6",
        "device_code_stddev_samp_status_bits",
        "status_bits_sum_2h",
        F.date_trunc("month", "reading_ts").alias("reading_ts_period"),
    )
)

with_windows = (
    filtered
    .select(
        "id",
        "device_code_stddev_samp_status_bits",
        F.lag("status_bits_rounded", 2).over(w5).alias("prev_status_bits_rounded"),
        F.row_number().over(w6).alias("row_seq"),
        F.rank().over(w7).alias("status_bits_sum_2h_rank"),
    )
)

result = (
    with_windows
    .select(
        "id",
        "device_code_stddev_samp_status_bits",
        "row_seq",
        "status_bits_sum_2h_rank",
    )
)

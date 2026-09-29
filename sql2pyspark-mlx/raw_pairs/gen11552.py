from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("temp_c_rounded", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_part = Window.partitionBy(F.year("reading_ts")).orderBy("temp_c_rounded", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_running = Window.orderBy("status_bits", "id")
w4 = Window.partitionBy(F.year("reading_ts"))
w5 = Window.orderBy("greatest_first_known_pressure_last_known_pressure", "id").rowsBetween(-2, Window.currentRow)
w6 = Window.orderBy("humidity", "id")
w7 = Window.orderBy("adjusted_pressure", "id")

cleaned = (
    df_device_readings
    .filter(F.col("reading_ts") >= F.lit("2025-09-01 06:00:00").cast("timestamp"))
    .select(
        "id",
        "reading_ts",
        "humidity",
        "pressure",
        "status_bits",
        F.round("temp_c", 0).alias("temp_c_rounded"),
        F.round((F.col("pressure") * 1.07), 2).alias("adjusted_pressure"),
    )
)

scored = (
    cleaned
    .filter(F.col("adjusted_pressure").between(22, 48))
    .select(
        "id",
        "reading_ts",
        "humidity",
        "pressure",
        "adjusted_pressure",
        F.first("pressure", ignorenulls=True).over(w).alias("first_known_pressure"),
        F.last("pressure", ignorenulls=True).over(w_part).alias("last_known_pressure"),
        F.ntile(4).over(w_running).alias("status_bits_bucket"),
    )
)

filtered = (
    scored
    .filter(F.col("reading_ts") > F.to_date(F.lit("2025-09-01")))
    .select(
        "id",
        "reading_ts",
        "humidity",
        "pressure",
        "adjusted_pressure",
        "status_bits_bucket",
        F.coalesce("last_known_pressure", F.lit(0)).alias("last_known_pressure_filled"),
        F.greatest("first_known_pressure", "last_known_pressure").alias("greatest_first_known_pressure_last_known_pressure"),
    )
)

with_windows = (
    filtered
    .select(
        "*",
        F.stddev_samp("humidity").over(w4).alias("reading_ts_year_stddev_samp_humidity"),
        F.avg("adjusted_pressure").over(w5).alias("adjusted_pressure_avg_2"),
        F.lead("status_bits_bucket", 2, 0).over(w6).alias("next_status_bits_bucket"),
    )
)

scored_device_readings = (
    with_windows
    .select(
        "id",
        "humidity",
        "adjusted_pressure",
        "status_bits_bucket",
        "last_known_pressure_filled",
        "greatest_first_known_pressure_last_known_pressure",
        "reading_ts_year_stddev_samp_humidity",
        "adjusted_pressure_avg_2",
        F.lag("last_known_pressure_filled").over(w7).alias("prev_last_known_pressure_filled"),
        F.lead("last_known_pressure_filled").over(w7).alias("next_last_known_pressure_filled"),
        F.lag("status_bits_bucket").over(w7).alias("prev_status_bits_bucket"),
        F.lead("status_bits_bucket").over(w7).alias("next_status_bits_bucket_2"),
        F.row_number().over(w7).alias("row_seq"),
    )
)

result = (
    scored_device_readings
    .select(
        "id",
        "humidity",
        "adjusted_pressure",
        "status_bits_bucket",
        "reading_ts_year_stddev_samp_humidity",
        "adjusted_pressure_avg_2",
        "prev_last_known_pressure_filled",
        "next_last_known_pressure_filled",
        "next_status_bits_bucket_2",
    )
    .orderBy(F.col("next_last_known_pressure_filled").asc_nulls_last())
)

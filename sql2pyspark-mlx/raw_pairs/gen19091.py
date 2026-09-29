from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code")
w2 = Window.partitionBy("device_code").orderBy(F.col("reading_ts").cast("bigint")).rangeBetween(-1800, Window.currentRow)
w3 = Window.partitionBy("device_code").orderBy("status_bits_rounded", "id")
w4 = Window.partitionBy(F.year("reading_ts_date")).orderBy(F.col("reading_ts").cast("bigint")).rangeBetween(-1800, Window.currentRow)
w_running = Window.orderBy("reading_ts", "id")
w6 = Window.orderBy("prev_humidity", "id")
w7 = Window.orderBy("corr_pressure_humidity", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w8 = Window.orderBy("corr_pressure_humidity", "id").rowsBetween(-6, Window.currentRow)
w9 = Window.orderBy(F.col("corr_pressure_humidity").desc(), "id")
w10 = Window.orderBy("seq_no", "id")

filtered = (
    df_device_readings
    .filter(F.col("reading_ts") > F.lit("2025-09-01 07:00:00").cast("timestamp"))
    .select(
        "*",
        F.round("status_bits", 1).alias("status_bits_rounded"),
        F.to_date("reading_ts").alias("reading_ts_date"),
    )
)

enriched = (
    filtered
    .select(
        "id",
        "reading_ts",
        "temp_c",
        "humidity",
        "pressure",
        "reading_ts_date",
        F.corr("pressure", "humidity").over(w).alias("corr_pressure_humidity"),
        F.avg("status_bits").over(w2).alias("status_bits_avg_30m"),
        F.row_number().over(w3).alias("event_seq"),
        F.round((F.col("temp_c") / F.sum("temp_c").over(w)), 4).alias("temp_c_share"),
    )
)

enriched_device_readings = (
    enriched
    .filter(F.to_date("reading_ts") >= "2023-10-01")
    .select(
        "id",
        "reading_ts",
        "temp_c",
        "humidity",
        "reading_ts_date",
        "corr_pressure_humidity",
        "status_bits_avg_30m",
        F.avg("corr_pressure_humidity").over(w4).alias("corr_pressure_humidity_avg_30m"),
        F.lag("humidity", 1).over(w_running).alias("prev_humidity"),
    )
)

base = (
    enriched_device_readings
    .filter(F.col("reading_ts") > F.lit("2025-09-01 04:00:00").cast("timestamp"))
    .withColumn("temp_c_times_humidity", (F.col("temp_c") * F.col("humidity")))
)

scored = (
    base
    .select(
        "id",
        "reading_ts",
        "temp_c",
        "status_bits_avg_30m",
        "corr_pressure_humidity_avg_30m",
        "prev_humidity",
        F.lag("corr_pressure_humidity_avg_30m", 1).over(w6).alias("prev_corr_pressure_humidity_avg_30m"),
        F.last("status_bits_avg_30m", ignorenulls=True).over(w7).alias("last_known_status_bits_avg_30m"),
        F.avg("temp_c_times_humidity").over(w8).alias("temp_c_times_humidity_avg_6"),
        F.row_number().over(w9).alias("seq_no"),
    )
)

with_windows = (
    scored
    .select(
        "*",
        F.lead("seq_no").over(w10).alias("next_seq_no"),
        F.lag("seq_no").over(w10).alias("prev_seq_no"),
        F.lag("last_known_status_bits_avg_30m").over(w10).alias("prev_last_known_status_bits_avg_30m"),
        F.row_number().over(w10).alias("event_seq"),
        F.lead("temp_c", 1).over(w10).alias("next_temp_c"),
    )
)

result = (
    with_windows
    .groupBy(F.date_trunc("HOUR", "reading_ts").alias("hour_window"))
    .agg(
        F.percentile_approx("next_temp_c", [0.25, 0.5, 0.75]).alias("next_temp_c_quartiles"),
        ((F.unix_timestamp(F.max("reading_ts")) - F.unix_timestamp(F.min("reading_ts"))) / 60).alias("span_minutes"),
        F.round(F.avg("corr_pressure_humidity_avg_30m"), 2).alias("avg_corr_pressure_humidity_avg_30m"),
    )
    .filter(F.col("avg_corr_pressure_humidity_avg_30m") > 0)
    .orderBy(F.col("hour_window").desc())
)

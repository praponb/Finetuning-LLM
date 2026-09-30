from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_running = Window.partitionBy("device_code").orderBy("status_bits", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("device_code")
w_recent = Window.partitionBy("device_code").orderBy("running_sum_pressure", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("device_code").orderBy("running_max_humidity_share", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy(F.date_trunc("week", "reading_ts"), "device_code").orderBy("running_sum_pressure", "id").rowsBetween(-2, -1)
w_grp = Window.partitionBy("device_code").orderBy("pressure", "id")
w7 = Window.partitionBy("device_code").orderBy(F.col("reading_ts").cast("bigint")).rangeBetween(-7200, Window.currentRow)
w8 = Window.partitionBy("device_code").orderBy("running_max_humidity_share", "id").rowsBetween(-6, -1)
w9 = Window.orderBy("pressure_avg_2_prior", "id")
w10 = Window.partitionBy("device_code").orderBy("running_temp_c", "id").rowsBetween(-6, Window.currentRow)

with_windows = (
    df_device_readings
    .select(
        "*",
        F.count("pressure").over(w_running).alias("running_count_pressure"),
        F.sum("pressure").over(w_running).alias("running_sum_pressure"),
        F.avg("pressure").over(w_running).alias("running_avg_pressure"),
        F.min("pressure").over(w_running).alias("running_min_pressure"),
        F.round((F.col("humidity") / F.sum("humidity").over(w2)), 4).alias("humidity_share"),
    )
)

with_windows_device_readings = (
    with_windows
    .select(
        "*",
        F.max("humidity_share").over(w_recent).alias("running_max_humidity_share"),
        F.min("humidity_share").over(w_recent).alias("running_min_humidity_share"),
        F.sum("humidity_share").over(w_recent).alias("running_sum_humidity_share"),
    )
)

metrics = (
    with_windows_device_readings
    .filter(F.col("device_code").isin(["DV-04", "DV-03", "DV-01"]))
    .select(
        "id",
        "device_code",
        "reading_ts",
        "temp_c",
        "pressure",
        "status_bits",
        "firmware",
        "running_count_pressure",
        "running_min_pressure",
        "humidity_share",
        "running_max_humidity_share",
        F.sum("temp_c").over(w4).alias("running_temp_c"),
        F.avg("pressure").over(w5).alias("pressure_avg_2_prior"),
        F.lag("running_sum_pressure", 1).over(w_grp).alias("prev_running_sum_pressure"),
        F.round((F.col("temp_c") / F.sum("temp_c").over(w2)), 4).alias("temp_c_share"),
        F.sum("humidity").over(w2).alias("device_code_sum_humidity"),
    )
)

enriched = (
    metrics
    .select(
        "*",
        F.stddev_samp("device_code_sum_humidity").over(w2).alias("device_code_stddev_samp_device_code_sum_humidity"),
        F.avg("running_max_humidity_share").over(w7).alias("running_max_humidity_share_avg_2h"),
        F.avg("running_min_pressure").over(w8).alias("running_min_pressure_avg_6_prior"),
        F.lead("running_count_pressure", 1).over(w9).alias("next_running_count_pressure"),
        F.avg("temp_c").over(w10).alias("temp_c_avg_6"),
    )
)

result = (
    enriched
    .select(
        "id",
        "device_code",
        "temp_c",
        "firmware",
        "running_count_pressure",
        "running_min_pressure",
        "humidity_share",
        "running_max_humidity_share",
        "running_temp_c",
    )
)

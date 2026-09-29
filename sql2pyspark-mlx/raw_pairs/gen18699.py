from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code")
w_grp = Window.orderBy("temp_c", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("device_code").orderBy("reading_ts", "id")
w4 = Window.partitionBy("device_code").orderBy("reading_ts", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy(F.year("reading_ts")).orderBy("pressure", "id").rowsBetween(-2, -1)
w6 = Window.partitionBy("device_code").orderBy(F.col("is_new_burst").desc_nulls_last())
w7 = Window.partitionBy("device_code").orderBy("temp_c", "id")

base = (
    df_device_readings
    .filter(F.col("reading_ts") >= F.lit("2025-09-01 02:00:00").cast("timestamp"))
    .select(
        "*",
        F.concat_ws("/", "firmware", "device_code").alias("firmware_device_code_key"),
    )
)

with_windows = (
    base
    .select(
        "*",
        F.corr("pressure", "status_bits").over(w).alias("corr_pressure_status_bits"),
        F.sum("status_bits").over(w_grp).alias("running_status_bits"),
    )
)

flagged = (
    with_windows
    .withColumn("is_new_burst", F.when(F.lag("reading_ts").over(w3).isNull() | (F.col("reading_ts") > F.lag("reading_ts").over(w3) + F.expr("INTERVAL 1 HOUR")), 1).otherwise(0))
)

islands = flagged.withColumn("session_no", F.sum("is_new_burst").over(w4))

with_windows_device_readings = (
    islands
    .filter(F.col("device_code").isin(["DV-01", "DV-03", "DV-02"]))
    .select(
        "id",
        "device_code",
        "temp_c",
        "status_bits",
        "firmware",
        "firmware_device_code_key",
        "running_status_bits",
        "session_no",
        F.avg("humidity").over(w5).alias("humidity_avg_2_prior"),
        F.rank().over(w6).alias("is_new_burst_rank"),
        F.lag("pressure", 1, 0).over(w7).alias("prev_pressure"),
        F.round((F.col("pressure") / F.sum("pressure").over(w)), 4).alias("pressure_share"),
    )
)

result = (
    with_windows_device_readings
    .select(
        "id",
        "device_code",
        "temp_c",
        "firmware",
        "firmware_device_code_key",
        "running_status_bits",
        "session_no",
        "humidity_avg_2_prior",
        "is_new_burst_rank",
        F.when(F.col("device_code").rlike("^(DV-02|DV-01)$"), "GROUP_B").when(F.col("device_code").rlike("^(DV-03|DV-04)$"), "EXTENDED").otherwise("PRIORITY").alias("device_code_zone"),
    )
    .orderBy(F.col("humidity_avg_2_prior").desc_nulls_last())
)

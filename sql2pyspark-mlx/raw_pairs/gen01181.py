from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code").orderBy("reading_ts", "id")
w2 = Window.partitionBy("device_code").orderBy("reading_ts", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy(F.to_date("reading_ts"), "device_code")
w4 = Window.partitionBy(F.date_trunc("YEAR", "reading_ts"), "device_code").orderBy(F.col("temp_c_rounded").desc_nulls_last())
w5 = Window.partitionBy("device_code")

filtered = (
    df_device_readings
    .filter((F.col("reading_ts") >= F.lit("2025-09-01").cast("date")) & (F.col("temp_c") < 31))
    .select(
        "*",
        (F.col("humidity") - F.col("temp_c")).alias("humidity_minus_temp_c"),
        F.round("pressure", 0).alias("pressure_rounded"),
    )
)

with_gaps = (
    filtered
    .withColumn("is_new_burst", F.when(F.col("reading_ts") - F.lag("reading_ts").over(w) > F.expr("INTERVAL 1 HOUR"), 1).otherwise(0))
)

islands = (
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

matched_keys = (
    islands
    .select("firmware")
    .intersectAll(
        df_device_readings
        .filter(F.col("humidity") < 43)
        .select("firmware")
    )
)

reconciled = (
    islands
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.firmware") == F.col("k.firmware")), "inner")
    .select("s.*")
)

valid = (
    reconciled
    .filter(F.col("firmware").isin(["2.0.1", "1.1.0"]))
    .select(
        "device_code",
        "reading_ts",
        "temp_c",
        "pressure",
        "firmware",
        F.col("reading_ts").cast("date").alias("reading_ts_day"),
    )
)

filtered_device_readings = (
    valid
    .filter(F.col("reading_ts_day") < "2025-05-20")
    .select(
        "*",
        F.round("temp_c", 1).alias("temp_c_rounded"),
        F.concat_ws("-", "firmware", "device_code").alias("firmware_device_code_key"),
    )
)

enriched = (
    filtered_device_readings
    .filter(F.col("temp_c_rounded").between(47, 76))
    .select(
        "*",
        F.stddev_samp("pressure").over(w3).alias("reading_ts_to_date_stddev_samp_pressure"),
    )
)

metrics = (
    enriched
    .withColumn("temp_c_rounded_cume_dist", F.cume_dist().over(w4))
    .withColumn("device_code_max_pressure", F.max("pressure").over(w5))
    .withColumn("corr_pressure_temp_c", F.corr("pressure", "temp_c").over(w5))
)

result = (
    metrics
    .select("device_code", "pressure", "temp_c")
    .unpivot(
        ids=["device_code"],
        values=["pressure", "temp_c"],
        variableColumnName="metric_name",
        valueColumnName="val",
    )
    .filter(F.col("val").isNotNull())
)

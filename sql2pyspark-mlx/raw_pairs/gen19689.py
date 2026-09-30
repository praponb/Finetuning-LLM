from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_part = Window.partitionBy("device_code").orderBy("humidity_rounded_minus_temp_c", "id").rowsBetween(-3, Window.currentRow)
w_grp = Window.partitionBy("device_code").orderBy("humidity_rounded_minus_temp_c", "id")
w3 = Window.partitionBy("device_code").orderBy("pressure", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("device_code").orderBy(F.col("humidity_rounded_minus_temp_c_bucket_filled").desc(), "id")

filtered = (
    df_device_readings
    .filter(F.col("reading_ts") > F.lit("2025-09-01").cast("date"))
    .select(
        "id",
        "device_code",
        "reading_ts",
        "temp_c",
        "pressure",
        "firmware",
        F.round("humidity", 2).alias("humidity_rounded"),
    )
)

filtered_device_readings = (
    filtered
    .filter((F.col("reading_ts") >= F.to_date(F.lit("2025-09-01"))) & (F.col("firmware") == "1.1.0"))
    .select(
        "id",
        "device_code",
        "temp_c",
        "pressure",
        "firmware",
        F.when(F.col("humidity_rounded") >= 66, "High").when(F.col("humidity_rounded") >= 34, "Medium").otherwise("Low").alias("humidity_rounded_tier"),
        (F.col("humidity_rounded") - F.col("temp_c")).alias("humidity_rounded_minus_temp_c"),
    )
)

with_windows = (
    filtered_device_readings
    .filter(F.col("pressure") < 998)
    .select(
        "id",
        "device_code",
        "temp_c",
        "pressure",
        F.avg("pressure").over(w_part).alias("pressure_avg_3"),
        F.ntile(10).over(w_grp).alias("humidity_rounded_minus_temp_c_bucket"),
        F.sum("pressure").over(w3).alias("running_pressure"),
    )
)

filtered_device_readings_2 = (
    with_windows
    .filter(F.col("pressure").between(1000, 1006))
    .select(
        "*",
        F.coalesce("humidity_rounded_minus_temp_c_bucket", F.lit(0)).alias("humidity_rounded_minus_temp_c_bucket_filled"),
    )
)

ranked = (
    filtered_device_readings_2
    .withColumn("rank_in_group", F.row_number().over(w4))
    .filter(F.col("rank_in_group") == 1)
)

result = (
    ranked
    .select(
        "id",
        "device_code",
        "temp_c",
        "pressure",
        "humidity_rounded_minus_temp_c_bucket",
        "running_pressure",
        "humidity_rounded_minus_temp_c_bucket_filled",
    )
    .orderBy(
        F.col("humidity_rounded_minus_temp_c_bucket_filled").desc_nulls_first(),
        F.col("device_code").desc(),
    )
)

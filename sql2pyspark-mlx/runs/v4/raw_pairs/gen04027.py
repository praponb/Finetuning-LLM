from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code").orderBy("reading_ts", "id")
w2 = Window.partitionBy("device_code").orderBy("reading_ts", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("device_code").orderBy(F.col("pressure").asc_nulls_last(), "id")

valid = (
    df_device_readings
    .filter(F.col("reading_ts") >= "2023-08-01")
    .select(
        "id",
        "device_code",
        "reading_ts",
        "temp_c",
        "humidity",
        "pressure",
        "status_bits",
        "firmware",
        F.when(F.col("temp_c") >= 34.7, "High").when(F.col("temp_c") >= 26.6, "Medium").otherwise("Low").alias("temp_c_level"),
    )
)

with_gaps = (
    valid
    .withColumn("is_new_session", F.when(F.col("reading_ts") - F.lag("reading_ts").over(w) > F.expr("INTERVAL 30 MINUTES"), 1).otherwise(0))
)

sessionized = with_gaps.withColumn("burst_id", F.sum("is_new_session").over(w2))

latest = (
    sessionized
    .withColumn("rn", F.row_number().over(w3))
    .filter(F.col("rn") == 1)
)

base = (
    latest
    .filter(F.to_date("reading_ts") < "2025-03-24")
    .select(
        "id",
        "device_code",
        "reading_ts",
        "temp_c",
        "humidity",
        "pressure",
        "status_bits",
        "firmware",
        "is_new_session",
        "rn",
        F.col("rn").cast("double").alias("rn_num"),
        F.round((F.col("temp_c") / F.col("pressure")), 2).alias("temp_c_per_pressure"),
    )
)

result = (
    base
    .groupBy(
        F.when(F.col("status_bits") >= 11, "PREMIUM").otherwise("STANDARD").alias("status_bits_tier"),
    )
    .agg(
        F.count(F.when(F.col("device_code") == "DV-03", 1)).alias("dv_03_count"),
        F.corr("humidity", "temp_c").alias("corr_humidity_temp_c"),
        F.count("*").alias("num_rows"),
        F.corr("pressure", "temp_c").alias("corr_pressure_temp_c"),
        F.min("reading_ts").alias("started_at"),
    )
)

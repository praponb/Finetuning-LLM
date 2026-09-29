from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code").orderBy("reading_ts", "id")
w2 = Window.partitionBy("device_code").orderBy("reading_ts", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

valid = (
    df_device_readings
    .filter(F.col("firmware") == "2.0.1")
    .select(
        "*",
        F.coalesce("humidity", F.lit(0)).alias("humidity_filled"),
        F.lower("firmware").alias("firmware_lower"),
    )
)

gaps = (
    valid
    .withColumn("is_new_session", F.when(F.lag("reading_ts").over(w).isNull() | (F.col("reading_ts") > F.lag("reading_ts").over(w) + F.expr("INTERVAL 1 HOUR")), 1).otherwise(0))
)

sessionized = (
    gaps
    .select(
        "id",
        "device_code",
        "reading_ts",
        "pressure",
        "status_bits",
        "firmware",
        F.sum("is_new_session").over(w2).alias("island_id"),
    )
)

result = (
    sessionized
    .groupBy(F.date_trunc("HOUR", "reading_ts").alias("hour_window"))
    .agg(
        F.percentile_approx("status_bits", 0.5).alias("status_bits_p50"),
        F.sum(F.when(F.col("device_code") == "DV-03", F.col("island_id")).otherwise(0)).alias("dv_03_island_id"),
        F.count("*").alias("row_count"),
        F.percentile_approx("pressure", 0.9).alias("pressure_p90"),
    )
    .filter(F.col("row_count") > 1)
    .orderBy(F.col("hour_window").desc())
)

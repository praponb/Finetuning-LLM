from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code").orderBy("reading_ts", "id")
w2 = Window.partitionBy("device_code").orderBy("reading_ts", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

valid = (
    df_device_readings
    .filter((F.col("reading_ts") >= F.lit("2025-09-01 03:00:00").cast("timestamp")) & (F.col("reading_ts") >= "2025-09-20"))
    .select(
        "*",
        F.when(F.col("pressure") >= 1019, "High").when(F.col("pressure") >= 990, "Medium").otherwise("Low").alias("pressure_tier"),
    )
)

with_prev = valid.withColumn("prev_reading_ts", F.lag("reading_ts").over(w))

with_gaps = (
    with_prev
    .withColumn("is_new_session", F.when(F.col("prev_reading_ts").isNull() | (F.col("reading_ts") > F.col("prev_reading_ts") + F.expr("INTERVAL 15 MINUTES")), 1).otherwise(0))
)

sessions_device_readings = (
    with_gaps
    .select(
        "id",
        "device_code",
        "reading_ts",
        "temp_c",
        "pressure",
        "firmware",
        F.sum("is_new_session").over(w2).alias("island_id"),
    )
)

result = (
    sessions_device_readings
    .groupBy(
        F.when(F.col("island_id") >= 9, "LARGE").otherwise("SMALL").alias("island_id_tier"),
    )
    .agg(
        F.percentile_approx("island_id", [0.25, 0.5, 0.75]).alias("island_id_quartiles"),
        F.percentile_approx("pressure", [0.25, 0.5, 0.75]).alias("pressure_quartiles"),
    )
    .orderBy(F.col("island_id_tier").desc())
)

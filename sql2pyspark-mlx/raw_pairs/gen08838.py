from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code").orderBy("reading_ts", "id")
w2 = Window.partitionBy("device_code").orderBy("reading_ts", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

valid = (
    df_device_readings
    .filter(F.col("reading_ts") > F.lit("2025-09-01").cast("date"))
    .select(
        "*",
        F.round("pressure", 1).alias("pressure_rounded"),
        F.coalesce("pressure", F.lit(0)).alias("pressure_filled"),
    )
)

prev_events = valid.withColumn("prev_reading_ts", F.lag("reading_ts").over(w))

flagged = (
    prev_events
    .withColumn("new_group_flag", F.when(F.col("prev_reading_ts").isNull() | (F.col("reading_ts") > F.col("prev_reading_ts") + F.expr("INTERVAL 20 MINUTES")), 1).otherwise(0))
)

sessions_device_readings = (
    flagged
    .select(
        "id",
        "device_code",
        "reading_ts",
        "humidity",
        "pressure",
        "status_bits",
        "pressure_rounded",
        "pressure_filled",
        F.sum("new_group_flag").over(w2).alias("island_id"),
    )
)

result = (
    sessions_device_readings
    .groupBy("island_id")
    .agg(
        F.sum(F.when(F.col("device_code") == "DV-02", F.col("pressure")).otherwise(0)).alias("dv_02_pressure"),
        F.countDistinct("id").alias("distinct_ids"),
        F.count("*").alias("cnt"),
    )
    .orderBy(F.col("cnt").desc())
)

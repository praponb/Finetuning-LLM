from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code").orderBy("reading_ts", "id")
w2 = Window.partitionBy("device_code").orderBy("reading_ts", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("session_no").orderBy(F.col("avg_pressure_int").desc())

cleaned = (
    df_device_readings
    .filter(F.col("status_bits") >= 9)
    .select(
        "id",
        "device_code",
        "reading_ts",
        "status_bits",
        F.col("pressure").cast("int").alias("pressure_int"),
        F.substring("device_code", 1, 3).alias("device_code_prefix"),
    )
)

prev_events = cleaned.withColumn("prev_reading_ts", F.lag("reading_ts").over(w))

gaps = (
    prev_events
    .withColumn("is_new_session", F.when(F.col("prev_reading_ts").isNull() | (F.col("reading_ts") > F.col("prev_reading_ts") + F.expr("INTERVAL 15 MINUTES")), 1).otherwise(0))
)

sessions_device_readings = gaps.withColumn("session_no", F.sum("is_new_session").over(w2))

result = (
    sessions_device_readings
    .groupBy(
        "session_no",
        F.when(F.col("session_no") >= 7, "LARGE").otherwise("SMALL").alias("session_no_tier"),
    )
    .agg(
        F.countDistinct("id").alias("distinct_ids"),
        F.round(F.avg("pressure_int"), 2).alias("avg_pressure_int"),
        F.round(F.sum("status_bits") / F.nullif(F.count(F.when(F.col("device_code") == "DV-03", 1)), F.lit(0)), 2).alias("status_bits_per_dv_03"),
    )
    .withColumn("avg_pressure_int_percent_rank", F.percent_rank().over(w3))
    .orderBy(F.col("distinct_ids").asc_nulls_first())
)

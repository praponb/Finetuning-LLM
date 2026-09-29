from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code").orderBy("reading_ts", "id")
w2 = Window.partitionBy("device_code").orderBy("reading_ts", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

cleaned = (
    df_device_readings
    .filter(F.col("reading_ts") >= F.lit("2025-09-01 06:00:00").cast("timestamp"))
    .select(
        "id",
        "device_code",
        "reading_ts",
        "temp_c",
        "humidity",
        "pressure",
        "status_bits",
        F.to_date("reading_ts").alias("reading_ts_date"),
        F.round("pressure", 1).alias("pressure_rounded"),
    )
)

flagged = (
    cleaned
    .withColumn("session_start_flag", F.when(F.col("reading_ts") - F.lag("reading_ts").over(w) > F.expr("INTERVAL 1 HOUR"), 1).otherwise(0))
)

sessions_device_readings = flagged.withColumn("session_no", F.sum("session_start_flag").over(w2))

result = (
    sessions_device_readings
    .groupBy("session_no")
    .agg(
        F.min("reading_ts").alias("first_seen"),
        F.sort_array(F.collect_set("device_code")).alias("device_code_list"),
        F.countDistinct("id").alias("distinct_ids"),
        F.sum(F.when(F.col("device_code") == "DV-01", F.col("pressure_rounded"))).alias("dv_01_total_pressure_rounded"),
        F.count(F.when(F.col("device_code") == "DV-03", 1)).alias("dv_03_rows"),
        F.count(F.when(F.col("device_code") == "DV-02", 1)).alias("dv_02_count"),
    )
    .orderBy(F.col("dv_03_rows").asc_nulls_first())
)

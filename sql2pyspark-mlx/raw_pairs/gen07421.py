from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code").orderBy("reading_ts", "id")
w2 = Window.partitionBy("device_code").orderBy("reading_ts", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

cleaned = (
    df_device_readings
    .filter(F.col("reading_ts") >= F.lit("2025-09-01").cast("date"))
    .withColumn("reading_ts_date", F.to_date("reading_ts"))
    .withColumn("pressure_plus_temp_c", (F.col("pressure") + F.col("temp_c")))
)

flagged = (
    cleaned
    .withColumn("session_start_flag", F.when(F.lag("reading_ts").over(w).isNull() | (F.col("reading_ts") > F.lag("reading_ts").over(w) + F.expr("INTERVAL 30 MINUTES")), 1).otherwise(0))
)

sessionized = (
    flagged
    .select(
        "id",
        "device_code",
        "reading_ts",
        "temp_c",
        "pressure",
        "reading_ts_date",
        "pressure_plus_temp_c",
        F.sum("session_start_flag").over(w2).alias("burst_id"),
    )
)

result = (
    sessionized
    .withColumn("pressure_plus_temp_c_tier", F.when(F.col("pressure_plus_temp_c") >= 82, "PREMIUM").otherwise("STANDARD"))
    .withColumn("activity_month", F.date_trunc("MONTH", "reading_ts_date"))
    .withColumn("week_start", F.date_trunc("WEEK", "reading_ts_date"))
    .groupingSets(
        [["pressure_plus_temp_c_tier", "activity_month", "week_start"], ["pressure_plus_temp_c_tier", "activity_month"], []],
        "pressure_plus_temp_c_tier",
        "activity_month",
        "week_start",
    )
    .agg(
        F.round(F.avg("pressure"), 2).alias("avg_pressure"),
        F.count(F.when(F.col("device_code") == "DV-01", 1)).alias("dv_01_rows"),
        F.min("reading_ts").alias("started_at"),
        F.grouping("pressure_plus_temp_c_tier").alias("grouping_id"),
    )
    .filter(F.col("avg_pressure") > 0)
)

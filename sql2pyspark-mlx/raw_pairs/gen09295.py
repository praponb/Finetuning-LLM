from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code").orderBy("reading_ts", "id")
w2 = Window.partitionBy("device_code").orderBy("reading_ts", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

flagged = (
    df_device_readings
    .withColumn("is_new_session", F.when(F.lag("reading_ts").over(w).isNull() | (F.col("reading_ts") > F.lag("reading_ts").over(w) + F.expr("INTERVAL 1 HOUR")), 1).otherwise(0))
)

sessions_device_readings = (
    flagged
    .select(
        "id",
        "device_code",
        "reading_ts",
        "pressure",
        "status_bits",
        "firmware",
        F.sum("is_new_session").over(w2).alias("burst_id"),
    )
)

result = (
    sessions_device_readings
    .groupBy(
        F.date_trunc("HOUR", "reading_ts").alias("hour_window"),
        F.date_trunc("DAY", "reading_ts").alias("activity_day"),
    )
    .agg(
        F.max("reading_ts").alias("last_reading_ts"),
        F.round(F.sum("pressure") / F.nullif(F.count(F.when(F.col("device_code") == "DV-01", 1)), F.lit(0)), 2).alias("pressure_per_dv_01"),
        F.count(F.when(F.col("device_code") == "DV-02", 1)).alias("dv_02_rows"),
        F.bit_and("status_bits").alias("status_bits_bit_and"),
        F.percentile_approx("burst_id", [0.25, 0.5, 0.75]).alias("burst_id_quartiles"),
        F.max(F.when(F.col("device_code") == "DV-03", F.col("burst_id"))).alias("dv_03_max_burst_id"),
    )
    .orderBy("dv_02_rows")
)

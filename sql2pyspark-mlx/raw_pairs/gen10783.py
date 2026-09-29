from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code").orderBy("reading_ts", "id")
w2 = Window.partitionBy("device_code").orderBy("reading_ts", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

filtered = (
    df_device_readings
    .filter(F.col("humidity") < 45)
    .select(
        "id",
        "device_code",
        "reading_ts",
        "temp_c",
        "pressure",
        "firmware",
        F.date_format("reading_ts", "yyyy-MM").alias("reading_ts_label"),
    )
)

flagged = (
    filtered
    .withColumn("is_new_session", F.when(F.col("reading_ts") - F.lag("reading_ts").over(w) > F.expr("INTERVAL 45 MINUTES"), 1).otherwise(0))
)

islands = flagged.withColumn("session_id", F.sum("is_new_session").over(w2))

result = (
    islands
    .groupBy(
        F.when(F.col("pressure") >= 1024, "LARGE").otherwise("SMALL").alias("pressure_tier"),
        F.date_trunc("HOUR", "reading_ts").alias("hour_window"),
    )
    .agg(
        ((F.unix_timestamp(F.max("reading_ts")) - F.unix_timestamp(F.min("reading_ts"))) / 60).alias("duration_min"),
        F.round(F.avg("is_new_session"), 2).alias("avg_is_new_session"),
        F.stddev_samp("temp_c").alias("temp_c_stddev"),
    )
    .orderBy(F.col("temp_c_stddev").asc_nulls_last())
)

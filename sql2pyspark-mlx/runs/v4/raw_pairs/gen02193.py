from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code").orderBy("reading_ts", "id")
w2 = Window.partitionBy("device_code").orderBy("reading_ts", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

flagged = (
    df_device_readings
    .withColumn("is_new_session", F.when(F.lag("reading_ts").over(w).isNull() | (F.col("reading_ts") > F.lag("reading_ts").over(w) + F.expr("INTERVAL 20 MINUTES")), 1).otherwise(0))
)

sessionized = flagged.withColumn("session_id", F.sum("is_new_session").over(w2))

result = (
    sessionized
    .groupBy("device_code")
    .agg(
        F.bit_or("status_bits").alias("status_bits_bit_or"),
        F.percentile_approx("is_new_session", 0.9).alias("is_new_session_p90"),
        F.round(F.sum("session_id") / F.nullif(F.count(F.when(F.col("device_code") == "DV-03", 1)), F.lit(0)), 2).alias("session_id_per_dv_03"),
        F.stddev_samp("humidity").alias("humidity_stddev"),
        F.count(F.when(F.col("device_code") == "DV-02", 1)).alias("dv_02_count"),
        F.sum(F.when(F.col("device_code") == "DV-03", F.col("is_new_session")).otherwise(0)).alias("dv_03_is_new_session"),
    )
    .filter(F.col("is_new_session_p90") > 0)
    .orderBy(F.col("dv_03_is_new_session").desc_nulls_last())
)

from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code").orderBy("reading_ts", "id")
w2 = Window.partitionBy("device_code").orderBy("reading_ts", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("session_id").orderBy(F.col("corr_pressure_status_bits").desc())

gaps = (
    df_device_readings
    .withColumn("is_new_session", F.when(F.lag("reading_ts").over(w).isNull() | (F.col("reading_ts") > F.lag("reading_ts").over(w) + F.expr("INTERVAL 1 HOUR")), 1).otherwise(0))
)

sessionized = gaps.withColumn("session_id", F.sum("is_new_session").over(w2))

result = (
    sessionized
    .groupBy(
        "session_id",
        F.when(F.col("pressure") >= 1032, "PREMIUM").otherwise("STANDARD").alias("pressure_tier"),
    )
    .agg(
        F.percentile_approx("session_id", 0.5).alias("session_id_p50"),
        F.count(F.when(F.col("device_code") == "DV-01", 1)).alias("dv_01_rows"),
        F.sum(F.when(F.col("device_code") == "DV-04", F.col("is_new_session")).otherwise(0)).alias("dv_04_is_new_session"),
        F.corr("pressure", "status_bits").alias("corr_pressure_status_bits"),
        F.percentile_approx("temp_c", [0.25, 0.5, 0.75]).alias("temp_c_quartiles"),
    )
    .withColumn("corr_pressure_status_bits_percent_rank", F.percent_rank().over(w3))
)

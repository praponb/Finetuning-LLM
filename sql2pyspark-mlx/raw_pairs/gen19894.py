from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code").orderBy("reading_ts", "id")
w2 = Window.partitionBy("device_code").orderBy("reading_ts", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

with_gaps = (
    df_device_readings
    .withColumn("session_start_flag", F.when(F.lag("reading_ts").over(w).isNull() | (F.col("reading_ts") > F.lag("reading_ts").over(w) + F.expr("INTERVAL 15 MINUTES")), 1).otherwise(0))
)

islands = with_gaps.withColumn("island_id", F.sum("session_start_flag").over(w2))

result = (
    islands
    .groupBy(
        "device_code",
        F.when(F.col("pressure") >= 1029, "LARGE").otherwise("SMALL").alias("pressure_tier"),
    )
    .agg(
        F.min("reading_ts").alias("started_at"),
        F.avg(F.when(F.col("device_code") == "DV-01", F.col("pressure"))).alias("dv_01_avg_pressure"),
        F.sort_array(F.collect_set("device_code")).alias("device_code_list"),
        F.count(F.when(F.col("device_code") == "DV-01", 1)).alias("dv_01_count"),
    )
    .orderBy(F.col("dv_01_count").desc())
)

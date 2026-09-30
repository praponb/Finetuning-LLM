from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code").orderBy("reading_ts", "id")
w2 = Window.partitionBy("device_code").orderBy("reading_ts", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

gaps = (
    df_device_readings
    .withColumn("is_new_burst", F.when(F.col("reading_ts") - F.lag("reading_ts").over(w) > F.expr("INTERVAL 15 MINUTES"), 1).otherwise(0))
)

sessions_device_readings = gaps.withColumn("burst_id", F.sum("is_new_burst").over(w2))

result = (
    sessions_device_readings
    .groupBy("burst_id")
    .agg(
        F.round(F.sum("is_new_burst") / F.nullif(F.count(F.when(F.col("device_code") == "DV-01", 1)), F.lit(0)), 2).alias("is_new_burst_per_dv_01"),
        F.count(F.when(F.col("device_code") == "DV-01", 1)).alias("dv_01_rows"),
        F.percentile_approx("is_new_burst", [0.25, 0.5, 0.75]).alias("is_new_burst_quartiles"),
        F.count(F.when(F.col("device_code") == "DV-02", 1)).alias("dv_02_rows"),
        F.max(F.when(F.col("device_code") == "DV-02", F.col("pressure"))).alias("dv_02_max_pressure"),
    )
    .orderBy(F.col("dv_01_rows").asc_nulls_last())
)

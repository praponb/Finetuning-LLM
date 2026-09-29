from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code").orderBy("reading_ts", "id")
w2 = Window.partitionBy("device_code").orderBy("reading_ts", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("device_code").orderBy(F.col("new_group_flag").asc_nulls_last(), "id")
w4 = Window.partitionBy("device_code").orderBy(F.col("burst_id").desc(), "id")
w5 = Window.partitionBy("device_code")
w_grp = Window.partitionBy("device_code").orderBy(F.col("reading_ts").cast("bigint")).rangeBetween(-7200, Window.currentRow)
w_running = Window.orderBy("reading_ts", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

gaps = (
    df_device_readings
    .withColumn("new_group_flag", F.when(F.col("reading_ts") - F.lag("reading_ts").over(w) > F.expr("INTERVAL 45 MINUTES"), 1).otherwise(0))
)

sessionized = gaps.withColumn("burst_id", F.sum("new_group_flag").over(w2))

ranked = (
    sessionized
    .withColumn("rn", F.row_number().over(w3))
    .filter(F.col("rn") == 1)
)

deduped = (
    ranked
    .withColumn("rn_2", F.row_number().over(w4))
    .filter(F.col("rn_2") == 1)
    .drop("rn_2")
)

metrics = (
    deduped
    .select(
        "*",
        F.sum("humidity").over(w5).alias("device_code_sum_humidity"),
        F.max("pressure").over(w_grp).alias("pressure_max_2h"),
        F.sum("burst_id").over(w_running).alias("running_burst_id"),
    )
)

base = (
    metrics
    .filter(F.col("reading_ts") >= F.lit("2025-09-01 02:00:00").cast("timestamp"))
    .select("*", (F.col("pressure") + F.col("temp_c")).alias("pressure_plus_temp_c"))
)

totals = (
    base
    .groupBy("device_code", F.date_trunc("day", "reading_ts").alias("activity_day"))
    .agg(
        F.count(F.when(F.col("device_code") == "DV-01", 1)).alias("dv_01_rows"),
        F.count("*").alias("cnt"),
        F.count(F.when(F.col("device_code") == "DV-02", 1)).alias("dv_02_count"),
    )
)

result = totals.select("device_code", "activity_day", "dv_01_rows")

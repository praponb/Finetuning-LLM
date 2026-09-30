from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code").orderBy("pressure", "id")
w2 = Window.partitionBy("device_code").orderBy("pressure_per_humidity", "id").rowsBetween(-5, Window.currentRow)
w3 = Window.partitionBy("device_code").orderBy(F.col("reading_ts").cast("bigint")).rangeBetween(-900, Window.currentRow)
w_grp = Window.partitionBy(F.year("reading_ts"), "device_code").orderBy("temp_c", "id").rowsBetween(-3, -1)
w5 = Window.partitionBy("device_code").orderBy("pressure_plus_humidity", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.partitionBy("device_code").orderBy("reading_ts", "id")
w7 = Window.partitionBy("device_code").orderBy("reading_ts", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w8 = Window.partitionBy("device_code").orderBy(F.col("humidity").desc_nulls_last(), "id")
w9 = Window.partitionBy("device_code").orderBy(F.col("rn").desc())

filtered = (
    df_device_readings
    .filter(F.col("reading_ts") > F.lit("2025-09-01").cast("date"))
    .select(
        "id",
        "device_code",
        "reading_ts",
        "temp_c",
        "humidity",
        "pressure",
        "firmware",
        F.round((F.col("pressure") / F.col("humidity")), 2).alias("pressure_per_humidity"),
    )
)

base = (
    filtered
    .filter(F.col("reading_ts") >= F.lit("2025-09-01 08:00:00").cast("timestamp"))
    .withColumn("pressure_plus_humidity", (F.col("pressure") + F.col("humidity")))
    .withColumn("pressure_per_humidity_minus_pressure", (F.col("pressure_per_humidity") - F.col("pressure")))
)

with_windows = (
    base
    .select(
        "*",
        F.row_number().over(w).alias("event_seq"),
        F.avg("humidity").over(w2).alias("humidity_avg_5"),
        F.count("temp_c").over(w3).alias("temp_c_count_15m"),
    )
)

scored = (
    with_windows
    .filter(F.col("temp_c_count_15m") <= 12)
    .withColumn("humidity_avg_5_avg_3_prior", F.avg("humidity_avg_5").over(w_grp))
    .withColumn("cumulative_pressure_per_humidity", F.sum("pressure_per_humidity").over(w5))
)

with_gaps = (
    scored
    .withColumn("session_start_flag", F.when(F.lag("reading_ts").over(w6).isNull() | (F.col("reading_ts") > F.lag("reading_ts").over(w6) + F.expr("INTERVAL 45 MINUTES")), 1).otherwise(0))
)

sessions_device_readings = with_gaps.withColumn("session_id", F.sum("session_start_flag").over(w7))

ranked = (
    sessions_device_readings
    .withColumn("rn", F.row_number().over(w8))
    .filter(F.col("rn") == 1)
)

deduped = (
    ranked
    .withColumn("row_num", F.rank().over(w9))
    .filter(F.col("row_num") == 1)
)

result = (
    deduped
    .cube("session_id")
    .agg(
        F.sort_array(F.collect_set("device_code")).alias("device_code_list"),
        F.percentile_approx("temp_c_count_15m", 0.9).alias("temp_c_count_15m_p90"),
        F.round(F.avg("temp_c"), 2).alias("avg_temp_c"),
        F.count(F.when(F.col("device_code") == "DV-04", 1)).alias("dv_04_count"),
        F.round(F.avg("pressure"), 2).alias("avg_pressure"),
        F.sort_array(F.collect_set("device_code")).alias("device_code_list_2"),
    )
    .filter(F.col("temp_c_count_15m_p90") < 1000000)
    .orderBy(F.col("avg_temp_c").desc())
)

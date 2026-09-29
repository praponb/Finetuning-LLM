from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code").orderBy("reading_ts", "id")
w2 = Window.partitionBy("device_code").orderBy("reading_ts", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

filtered = (
    df_device_readings
    .filter(F.col("reading_ts") > F.to_timestamp(F.lit("2025-09-01 04:00:00")))
    .select("*", F.col("humidity").cast("int").alias("humidity_int"))
)

valid = (
    filtered
    .filter(F.col("reading_ts") > F.to_date(F.lit("2025-09-01")))
    .select("*", (F.col("pressure") * F.col("temp_c")).alias("pressure_times_temp_c"))
)

flagged = (
    valid
    .withColumn("is_new_session", F.when(F.col("reading_ts") - F.lag("reading_ts").over(w) > F.expr("INTERVAL 15 MINUTES"), 1).otherwise(0))
)

sessionized = (
    flagged
    .select(
        "id",
        "device_code",
        "reading_ts",
        "pressure_times_temp_c",
        F.sum("is_new_session").over(w2).alias("burst_id"),
    )
)

with_gaps = (
    sessionized
    .withColumn("is_new_session", F.when(F.lag("reading_ts").over(w).isNull() | (F.col("reading_ts") > F.lag("reading_ts").over(w) + F.expr("INTERVAL 30 MINUTES")), 1).otherwise(0))
)

sessionized_device_readings = with_gaps.withColumn("burst_id_2", F.sum("is_new_session").over(w2))

result = (
    sessionized_device_readings
    .groupBy(
        F.when(F.col("pressure_times_temp_c") >= 65, "PREMIUM").otherwise("STANDARD").alias("pressure_times_temp_c_tier"),
    )
    .agg(
        F.countDistinct("device_code").alias("distinct_device_codes"),
        F.sort_array(F.collect_set("device_code")).alias("device_code_list"),
        F.round(F.avg("burst_id_2"), 2).alias("avg_burst_id_2"),
    )
    .filter(F.col("distinct_device_codes") > 1)
    .orderBy("pressure_times_temp_c_tier")
)

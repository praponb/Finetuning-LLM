from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code").orderBy(F.col("humidity_filled").desc_nulls_last(), "id")
w2 = Window.partitionBy("device_code").orderBy(F.col("temp_c").desc(), "id")
w3 = Window.partitionBy("device_code").orderBy("reading_ts", "id")
w4 = Window.partitionBy("device_code").orderBy("reading_ts", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("device_code").orderBy("dedup_rank", "id").rowsBetween(-2, Window.currentRow)
w6 = Window.partitionBy("device_code").orderBy(F.col("island_id").desc())
w7 = Window.partitionBy("device_code").orderBy(F.col("last3_avg_burst_id").desc(), "id")

valid = (
    df_device_readings
    .filter(F.col("reading_ts") > F.lit("2025-09-01").cast("date"))
    .select("*", F.coalesce("humidity", F.lit(0)).alias("humidity_filled"))
)

ranked = (
    valid
    .withColumn("dedup_rank", F.row_number().over(w))
    .filter(F.col("dedup_rank") == 1)
)

ranked_device_readings = (
    ranked
    .withColumn("latest_rank", F.row_number().over(w2))
    .filter(F.col("latest_rank") == 1)
    .select("id", "device_code", "reading_ts", "temp_c", "firmware", "dedup_rank")
)

with_prev = ranked_device_readings.withColumn("prev_reading_ts", F.lag("reading_ts").over(w3))

with_gaps = (
    with_prev
    .withColumn("is_new_burst", F.when(F.col("prev_reading_ts").isNull() | (F.col("reading_ts") > F.col("prev_reading_ts") + F.expr("INTERVAL 30 MINUTES")), 1).otherwise(0))
)

sessions_device_readings = (
    with_gaps
    .select(
        "id",
        "device_code",
        "reading_ts",
        "temp_c",
        "dedup_rank",
        F.sum("is_new_burst").over(w4).alias("burst_id"),
    )
)

flagged = (
    sessions_device_readings
    .withColumn("is_new_session", F.when(F.col("reading_ts") - F.lag("reading_ts").over(w3) > F.expr("INTERVAL 20 MINUTES"), 1).otherwise(0))
)

sessionized = flagged.withColumn("island_id", F.sum("is_new_session").over(w4))

with_windows = (
    sessionized
    .filter(F.col("burst_id") < 7)
    .select(
        "id",
        "device_code",
        "reading_ts",
        "temp_c",
        "is_new_session",
        F.min("burst_id").over(w5).alias("last3_min_burst_id"),
        F.avg("burst_id").over(w5).alias("last3_avg_burst_id"),
        F.count("burst_id").over(w5).alias("last3_count_burst_id"),
        F.rank().over(w6).alias("island_id_rank"),
    )
)

ranked_device_readings_2 = (
    with_windows
    .withColumn("latest_rank", F.row_number().over(w7))
    .filter(F.col("latest_rank") == 1)
)

result = (
    ranked_device_readings_2
    .select("id", "device_code", "last3_avg_burst_id", "temp_c", "last3_min_burst_id")
    .unpivot(
        ids=["id", "device_code"],
        values=["last3_avg_burst_id", "temp_c", "last3_min_burst_id"],
        variableColumnName="metric_name",
        valueColumnName="reading",
    )
    .filter(F.col("reading").isNotNull())
)

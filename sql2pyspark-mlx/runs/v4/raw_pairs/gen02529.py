from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code").orderBy("temp_c_times_pressure")
w2 = Window.partitionBy("device_code").orderBy("reading_ts", "id")
w3 = Window.partitionBy("device_code").orderBy("reading_ts", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_part = Window.partitionBy("device_code").orderBy("status_bits_scaled", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("device_code").orderBy("temp_c_times_pressure", "id")

valid = (
    df_device_readings
    .filter(F.col("reading_ts") >= F.to_timestamp(F.lit("2025-09-01 03:00:00")))
    .select(
        "id",
        "device_code",
        "reading_ts",
        "humidity",
        "pressure",
        "status_bits",
        "firmware",
        (F.col("temp_c") * F.col("pressure")).alias("temp_c_times_pressure"),
    )
)

deduped = (
    valid
    .withColumn("rn", F.rank().over(w))
    .filter(F.col("rn") == 1)
)

cleaned = (
    deduped
    .filter(F.col("humidity") > 54)
    .select(
        "*",
        F.ceil("pressure").alias("pressure_whole"),
        F.round((F.col("status_bits") * 1.1), 2).alias("status_bits_scaled"),
    )
)

flagged = (
    cleaned
    .withColumn("new_group_flag", F.when(F.lag("reading_ts").over(w2).isNull() | (F.col("reading_ts") > F.lag("reading_ts").over(w2) + F.expr("INTERVAL 15 MINUTES")), 1).otherwise(0))
)

sessions_device_readings = flagged.withColumn("island_id", F.sum("new_group_flag").over(w3))

scored = (
    sessions_device_readings
    .select(
        "*",
        F.last("humidity", ignorenulls=True).over(w_part).alias("last_known_humidity"),
        F.lead("island_id", 1).over(w5).alias("next_island_id"),
    )
)

result = (
    scored
    .groupBy("firmware")
    .pivot("device_code", ["DV-01", "DV-03", "DV-02"])
    .agg(F.max("temp_c_times_pressure"))
    .select(
        "firmware",
        F.col("DV-01").alias("dv_01"),
        F.col("DV-03").alias("dv_03"),
        F.col("DV-02").alias("dv_02"),
    )
)

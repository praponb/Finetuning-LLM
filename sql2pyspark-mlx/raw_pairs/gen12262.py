from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_part = Window.partitionBy("device_code").orderBy("humidity", "id").rowsBetween(-2, Window.currentRow)
w2 = Window.partitionBy("device_code")
w3 = Window.partitionBy("device_code").orderBy("reading_ts", "id")
w4 = Window.partitionBy("device_code").orderBy("reading_ts", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

valid = (
    df_device_readings
    .filter(F.col("device_code") != "DV-04")
    .select(
        "id",
        "device_code",
        "reading_ts",
        "temp_c",
        "humidity",
        "pressure",
        "status_bits",
        "firmware",
        F.col("pressure").cast("int").alias("pressure_int"),
    )
)

enriched = (
    valid
    .filter(F.col("temp_c") >= 21)
    .select(
        "id",
        "device_code",
        "reading_ts",
        "temp_c",
        "pressure",
        "firmware",
        F.min("pressure").over(w_part).alias("last3_min_pressure"),
        F.count("pressure").over(w_part).alias("last3_count_pressure"),
        F.max("pressure").over(w_part).alias("last3_max_pressure"),
        F.sum("pressure").over(w_part).alias("last3_sum_pressure"),
        F.round((F.col("humidity") / F.sum("humidity").over(w2)), 4).alias("humidity_share"),
    )
)

cleaned = (
    enriched
    .filter(F.col("reading_ts") > F.to_timestamp(F.lit("2025-09-01 02:00:00")))
    .withColumn("days_since_reading_ts", F.datediff(F.current_date(), "reading_ts"))
)

flagged = (
    cleaned
    .withColumn("session_start_flag", F.when(F.lag("reading_ts").over(w3).isNull() | (F.col("reading_ts") > F.lag("reading_ts").over(w3) + F.expr("INTERVAL 45 MINUTES")), 1).otherwise(0))
)

islands = flagged.withColumn("session_no", F.sum("session_start_flag").over(w4))

matched_keys = (
    islands
    .select("device_code")
    .exceptAll(
        df_device_readings
        .filter(F.col("device_code") == "DV-01")
        .select("device_code")
    )
)

reconciled = (
    islands
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.device_code") == F.col("k.device_code")), "inner")
    .select("s.*")
)

result = (
    reconciled
    .select(
        "temp_c",
        "last3_min_pressure",
        "last3_count_pressure",
        "last3_max_pressure",
        "last3_sum_pressure",
        "humidity_share",
        "days_since_reading_ts",
        "session_no",
    )
    .orderBy(F.col("humidity_share").asc_nulls_first())
)

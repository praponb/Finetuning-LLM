from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code").orderBy("reading_ts", "id")
w2 = Window.partitionBy("device_code").orderBy("reading_ts", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("device_code").orderBy(F.col("reading_ts").cast("bigint")).rangeBetween(-1800, Window.currentRow)
w4 = Window.orderBy("pressure", "id").rowsBetween(-6, -1)
w5 = Window.orderBy("is_new_session", "id")
w6 = Window.partitionBy("device_code")

valid = (
    df_device_readings
    .filter((F.col("reading_ts") > F.lit("2025-09-01").cast("date")) & (F.col("status_bits") > 12))
    .withColumn("temp_c_whole", F.ceil("temp_c"))
)

flagged = (
    valid
    .withColumn("is_new_session", F.when(F.col("reading_ts") - F.lag("reading_ts").over(w) > F.expr("INTERVAL 30 MINUTES"), 1).otherwise(0))
)

sessionized = flagged.withColumn("island_id", F.sum("is_new_session").over(w2))

metrics = (
    sessionized
    .filter(F.col("firmware") != "2.0.1")
    .select(
        "id",
        "device_code",
        "temp_c",
        "pressure",
        "status_bits",
        "temp_c_whole",
        "is_new_session",
        F.count("status_bits").over(w3).alias("status_bits_count_30m"),
        F.avg("island_id").over(w4).alias("island_id_avg_6_prior"),
        F.row_number().over(w5).alias("event_seq"),
    )
)

matched_keys = (
    metrics
    .select("device_code", "id")
    .intersect(
        df_device_readings
        .filter(F.col("pressure") > 1013)
        .select("device_code", "id")
    )
)

reconciled = (
    metrics
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.device_code") == F.col("k.device_code")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

metrics_device_readings = (
    reconciled
    .select(
        "id",
        "device_code",
        "pressure",
        F.avg("status_bits").over(w6).alias("device_code_avg_status_bits"),
        F.max("status_bits").over(w6).alias("device_code_max_status_bits"),
    )
)

with_windows = (
    metrics_device_readings
    .select(
        "*",
        F.count("device_code_avg_status_bits").over(w6).alias("device_code_count_device_code_avg_status_bits"),
        F.max("device_code_max_status_bits").over(w6).alias("device_code_max_device_code_max_status_bits"),
    )
)

result = (
    with_windows
    .cube(
        F.when(F.col("pressure") >= 1033, "LARGE").otherwise("SMALL").alias("pressure_tier"),
        F.when(F.col("device_code_count_device_code_avg_status_bits") >= 13, "PREMIUM").otherwise("STANDARD").alias("device_code_count_device_code_avg_status_bits_tier"),
    )
    .agg(
        F.round(F.sum("device_code_max_status_bits") / F.nullif(F.count(F.when(F.col("device_code") == "DV-01", 1)), F.lit(0)), 2).alias("device_code_max_status_bits_per_dv_01"),
        F.round(F.avg("device_code_count_device_code_avg_status_bits"), 2).alias("avg_device_code_count_device_code_avg_status_bits"),
        F.count("*").alias("row_count"),
    )
)

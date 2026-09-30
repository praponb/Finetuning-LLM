from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code").orderBy("reading_ts", "id")
w2 = Window.partitionBy("device_code").orderBy("reading_ts", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("device_code")
w4 = Window.partitionBy("device_code").orderBy(F.col("reading_ts").cast("bigint")).rangeBetween(-900, Window.currentRow)

gaps = (
    df_device_readings
    .withColumn("is_new_burst", F.when(F.col("reading_ts") - F.lag("reading_ts").over(w) > F.expr("INTERVAL 1 HOUR"), 1).otherwise(0))
)

islands = gaps.withColumn("burst_id", F.sum("is_new_burst").over(w2))

remaining_keys = (
    islands
    .select("firmware", "id")
    .intersect(
        df_device_readings
        .filter(F.col("humidity") > 63)
        .select("firmware", "id")
    )
)

kept_rows = (
    islands
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.firmware") == F.col("k.firmware")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

metrics = (
    kept_rows
    .select(
        "*",
        F.max("burst_id").over(w3).alias("device_code_max_burst_id"),
        F.max("temp_c").over(w3).alias("device_code_max_temp_c"),
    )
)

metrics_device_readings = (
    metrics
    .select(
        "*",
        F.count("pressure").over(w4).alias("pressure_count_15m"),
        F.count("pressure").over(w3).alias("device_code_count_pressure"),
    )
)

result = (
    metrics_device_readings
    .select(
        "reading_ts",
        "humidity",
        "status_bits",
        "firmware",
        "burst_id",
        "device_code_max_burst_id",
        "device_code_count_pressure",
    )
)

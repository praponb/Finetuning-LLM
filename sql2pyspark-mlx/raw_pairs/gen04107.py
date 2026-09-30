from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code").orderBy("reading_ts", "id")
w2 = Window.partitionBy("device_code").orderBy("reading_ts", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

gaps = (
    df_device_readings
    .withColumn("is_new_burst", F.when(F.lag("reading_ts").over(w).isNull() | (F.col("reading_ts") > F.lag("reading_ts").over(w) + F.expr("INTERVAL 30 MINUTES")), 1).otherwise(0))
)

sessionized = (
    gaps
    .select(
        "id",
        "device_code",
        "reading_ts",
        "humidity",
        "pressure",
        F.sum("is_new_burst").over(w2).alias("island_id"),
    )
)

matched_keys = (
    sessionized
    .select("device_code", "id")
    .intersectAll(
        df_device_readings
        .filter(F.col("status_bits") <= 11)
        .select("device_code", "id")
    )
)

reconciled = (
    sessionized
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.device_code") == F.col("k.device_code")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

valid = (
    reconciled
    .filter(F.col("reading_ts") >= F.lit("2025-09-01 07:00:00").cast("timestamp"))
    .withColumn("reading_ts_day", F.col("reading_ts").cast("date"))
)

filtered = (
    valid
    .filter(F.col("humidity").between(43, 45))
    .select(
        "id",
        "humidity",
        "pressure",
        "reading_ts_day",
        F.col("reading_ts").cast("date").alias("reading_ts_day_2"),
        F.when(F.col("island_id") >= 6, "High").when(F.col("island_id") >= 2, "Medium").otherwise("Low").alias("island_id_tier"),
    )
)

totals = (
    filtered
    .groupBy(
        F.when(F.col("pressure") >= 1028, "HIGH").otherwise("LOW").alias("pressure_tier"),
    )
    .agg(
        F.count("*").alias("cnt"),
        F.countDistinct("id").alias("distinct_ids"),
        F.corr("humidity", "pressure").alias("corr_humidity_pressure"),
        F.round(F.avg("pressure"), 2).alias("avg_pressure"),
        F.corr("humidity", "pressure").alias("corr_humidity_pressure_2"),
    )
    .filter(F.col("cnt") > 1)
)

result = (
    totals
    .select(
        "pressure_tier",
        "cnt",
        "distinct_ids",
        "avg_pressure",
        "corr_humidity_pressure_2",
    )
)

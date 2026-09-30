from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code").orderBy("status_bits")
w2 = Window.partitionBy("device_code").orderBy(F.col("reading_ts").desc())
w3 = Window.partitionBy("device_code").orderBy("reading_ts", "id")
w4 = Window.partitionBy("device_code").orderBy("reading_ts", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

base = (
    df_device_readings
    .filter((F.col("reading_ts") >= F.to_date(F.lit("2025-09-01"))) & (F.col("status_bits") == 7))
    .withColumn("device_code_lower", F.lower("device_code"))
)

ranked = (
    base
    .withColumn("rank_in_group", F.rank().over(w))
    .filter(F.col("rank_in_group") == 1)
    .drop("rank_in_group")
)

ranked_device_readings = (
    ranked
    .withColumn("row_num", F.rank().over(w2))
    .filter(F.col("row_num") == 1)
    .select(
        "id",
        "device_code",
        "reading_ts",
        "temp_c",
        "pressure",
        "status_bits",
        "device_code_lower",
    )
)

with_prev = ranked_device_readings.withColumn("prev_reading_ts", F.lag("reading_ts").over(w3))

gaps = (
    with_prev
    .withColumn("new_group_flag", F.when(F.col("prev_reading_ts").isNull() | (F.col("reading_ts") > F.col("prev_reading_ts") + F.expr("INTERVAL 1 HOUR")), 1).otherwise(0))
)

sessionized = (
    gaps
    .select(
        "id",
        "device_code",
        "reading_ts",
        "pressure",
        "status_bits",
        "device_code_lower",
        F.sum("new_group_flag").over(w4).alias("burst_id"),
    )
)

base_device_readings = (
    sessionized
    .filter(F.col("reading_ts") > F.lit("2025-09-01").cast("date"))
    .withColumn("pressure_band", F.when(F.col("pressure") >= 1018, "High").when(F.col("pressure") >= 987, "Medium").otherwise("Low"))
)

kept_keys = (
    base_device_readings
    .select("id")
    .subtract(
        df_device_readings
        .filter(F.col("pressure").between(1003, 1018))
        .select("id")
    )
)

kept_rows = (
    base_device_readings
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

result = (
    kept_rows
    .select("reading_ts", "pressure", "burst_id")
    .orderBy(F.col("burst_id").desc())
)

from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code").orderBy("reading_ts", "id")
w2 = Window.partitionBy("device_code").orderBy("reading_ts", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_ordered = Window.partitionBy("device_code").orderBy("pressure", "id").rowsBetween(-2, Window.currentRow)

valid = (
    df_device_readings
    .filter(F.col("reading_ts") > F.lit("2025-09-01").cast("date"))
    .withColumn("status_bits_per_temp_c", F.round((F.col("status_bits") / F.col("temp_c")), 2))
    .withColumn("is_dv_01", F.when(F.col("device_code") == "DV-01", 1).otherwise(0))
)

gaps = (
    valid
    .withColumn("is_new_session", F.when(F.lag("reading_ts").over(w).isNull() | (F.col("reading_ts") > F.lag("reading_ts").over(w) + F.expr("INTERVAL 45 MINUTES")), 1).otherwise(0))
)

sessions_device_readings = (
    gaps
    .select(
        "id",
        "device_code",
        "reading_ts",
        "temp_c",
        "pressure",
        "status_bits",
        "firmware",
        "status_bits_per_temp_c",
        F.sum("is_new_session").over(w2).alias("session_no"),
    )
)

metrics = (
    sessions_device_readings
    .select(
        "id",
        "device_code",
        "temp_c",
        "pressure",
        "status_bits",
        "status_bits_per_temp_c",
        F.min("session_no").over(w_ordered).alias("last3_min_session_no"),
        F.sum("session_no").over(w_ordered).alias("last3_sum_session_no"),
        F.max("session_no").over(w_ordered).alias("last3_max_session_no"),
    )
)

kept_keys = (
    metrics
    .select("id")
    .intersect(
        df_device_readings
        .filter(F.col("reading_ts") < "2024-11-01")
        .select("id")
    )
)

reconciled = (
    metrics
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

result = (
    reconciled
    .select(
        "id",
        "device_code",
        "temp_c",
        "pressure",
        "status_bits",
        "last3_sum_session_no",
        "last3_max_session_no",
    )
    .orderBy(F.col("pressure").desc_nulls_first(), "last3_max_session_no")
)

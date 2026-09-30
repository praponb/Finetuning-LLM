from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code").orderBy("reading_ts", "id")
w2 = Window.partitionBy("device_code").orderBy("reading_ts", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("device_code").orderBy(F.col("reading_ts").desc(), "id")

filtered = (
    df_device_readings
    .filter(F.col("reading_ts") >= F.to_date(F.lit("2025-09-01")))
    .select(
        "*",
        F.col("pressure").cast("int").alias("pressure_int"),
        F.date_trunc("week", "reading_ts").alias("reading_ts_period"),
    )
)

valid = (
    filtered
    .filter(F.col("pressure_int") > 65)
    .select("*", F.round("temp_c", 2).alias("temp_c_rounded"))
)

gaps = (
    valid
    .withColumn("new_group_flag", F.when(F.lag("reading_ts").over(w).isNull() | (F.col("reading_ts") > F.lag("reading_ts").over(w) + F.expr("INTERVAL 45 MINUTES")), 1).otherwise(0))
)

sessions_device_readings = gaps.withColumn("burst_id", F.sum("new_group_flag").over(w2))

latest = (
    sessions_device_readings
    .withColumn("rank_in_group", F.row_number().over(w3))
    .filter(F.col("rank_in_group") == 1)
    .drop("rank_in_group")
)

gaps_2 = (
    latest
    .withColumn("is_new_burst", F.when(F.lag("reading_ts").over(w).isNull() | (F.col("reading_ts") > F.lag("reading_ts").over(w) + F.expr("INTERVAL 45 MINUTES")), 1).otherwise(0))
)

sessions_device_readings_2 = gaps_2.withColumn("session_no", F.sum("is_new_burst").over(w2))

matched_keys = (
    sessions_device_readings_2
    .select("id", "firmware")
    .intersect(
        df_device_readings
        .filter(F.col("status_bits") == 11)
        .select("id", "firmware")
    )
)

reconciled = (
    sessions_device_readings_2
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.firmware") == F.col("k.firmware")), "inner")
    .select("s.*")
)

result = (
    reconciled
    .select(
        "id",
        "reading_ts",
        "temp_c",
        "pressure",
        "status_bits",
        "pressure_int",
        "is_new_burst",
    )
    .orderBy(F.col("pressure_int").desc_nulls_last())
)

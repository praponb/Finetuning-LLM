from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_grp = Window.partitionBy("device_code").orderBy("reading_ts", "id").rowsBetween(-3, Window.currentRow)
w2 = Window.orderBy(F.col("pressure").asc_nulls_last())
w3 = Window.orderBy(F.col("reading_ts").cast("bigint")).rangeBetween(-1800, Window.currentRow)
w4 = Window.partitionBy("device_code").orderBy(F.col("greatest_temp_c_pressure").desc_nulls_last())
w5 = Window.orderBy(F.col("pressure").desc_nulls_first())

valid = (
    df_device_readings
    .filter(F.col("reading_ts") >= F.lit("2025-09-01").cast("date"))
    .withColumn("greatest_temp_c_pressure", F.greatest("temp_c", "pressure"))
)

with_windows = (
    valid
    .withColumn("last4_min_temp_c", F.min("temp_c").over(w_grp))
    .withColumn("last4_sum_temp_c", F.sum("temp_c").over(w_grp))
    .withColumn("last4_max_temp_c", F.max("temp_c").over(w_grp))
    .withColumn("pressure_rank", F.rank().over(w2))
)

filtered = (
    with_windows
    .filter(F.col("pressure").between(1016, 1019))
    .withColumn("last4_max_temp_c_plus_pressure_rank", (F.col("last4_max_temp_c") + F.col("pressure_rank")))
    .withColumn("status_bits_level", F.when(F.col("status_bits") >= 12, "High").when(F.col("status_bits") >= 3, "Medium").otherwise("Low"))
)

kept_keys = (
    filtered
    .select("id", "device_code")
    .subtract(
        df_device_readings
        .filter(F.col("status_bits") > 10)
        .select("id", "device_code")
    )
)

verified = (
    filtered
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.device_code") == F.col("k.device_code")), "inner")
    .select("s.*")
)

filtered_device_readings = (
    verified
    .filter(F.col("humidity") >= 49)
    .withColumn("pressure_band", F.when(F.col("pressure") >= 1022, "High").when(F.col("pressure") >= 999, "Medium").otherwise("Low"))
)

scored = (
    filtered_device_readings
    .select(
        "id",
        "device_code",
        "pressure",
        "last4_min_temp_c",
        "pressure_rank",
        "last4_max_temp_c_plus_pressure_rank",
        "pressure_band",
        F.count("last4_min_temp_c").over(w3).alias("last4_min_temp_c_count_30m"),
        F.rank().over(w4).alias("greatest_temp_c_pressure_rank"),
    )
)

scored_device_readings = (
    scored
    .select(
        "pressure",
        "last4_min_temp_c",
        "pressure_rank",
        "last4_min_temp_c_count_30m",
        "greatest_temp_c_pressure_rank",
        F.dense_rank().over(w5).alias("pressure_dense_rank"),
    )
)

result = (
    scored_device_readings
    .select("last4_min_temp_c", "pressure_rank", "greatest_temp_c_pressure_rank")
)

from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code").orderBy(F.col("status_bits").desc())
w2 = Window.orderBy("pressure_int", "id").rowsBetween(-5, -1)
w3 = Window.partitionBy("device_code").orderBy("status_bits", "id").rowsBetween(-5, -1)
w4 = Window.partitionBy("device_code").orderBy("pressure_per_status_bits", "id")
w5 = Window.partitionBy("device_code").orderBy("status_bits_plus_temp_c", "id")
w6 = Window.orderBy(F.col("pressure").desc_nulls_first())

filtered = (
    df_device_readings
    .filter(F.col("reading_ts") >= "2025-09-19")
    .select(
        "id",
        "device_code",
        "temp_c",
        "pressure",
        "status_bits",
        "firmware",
        (F.col("pressure") / F.nullif("status_bits", F.lit(0))).alias("pressure_per_status_bits"),
    )
)

filtered_device_readings = (
    filtered
    .filter(~F.col("device_code").isin(["DV-02", "DV-04", "DV-03"]))
    .withColumn("status_bits_plus_temp_c", (F.col("status_bits") + F.col("temp_c")))
    .withColumn("temp_c_adjusted", F.round((F.col("temp_c") * 1.15), 2))
)

filtered_device_readings_2 = (
    filtered_device_readings
    .filter(F.col("status_bits_plus_temp_c").between(40, 54))
    .withColumn("pressure_int", F.col("pressure").cast("int"))
)

deduped = (
    filtered_device_readings_2
    .withColumn("rn", F.rank().over(w))
    .filter(F.col("rn") == 1)
)

scored = (
    deduped
    .select(
        "*",
        F.avg("temp_c").over(w2).alias("temp_c_avg_5_prior"),
        F.avg("pressure").over(w3).alias("pressure_avg_5_prior"),
        F.lag("pressure", 1).over(w4).alias("prev_pressure"),
        F.row_number().over(w5).alias("row_seq"),
        F.dense_rank().over(w6).alias("pressure_dense_rank"),
    )
)

kept_keys = (
    scored
    .select("device_code", "firmware")
    .intersect(
        df_device_readings
        .filter(F.col("device_code") == "DV-03")
        .select("device_code", "firmware")
    )
)

reconciled = (
    scored
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.device_code") == F.col("k.device_code")) & (F.col("s.firmware") == F.col("k.firmware")), "inner")
    .select("s.*")
)

cleaned = (
    reconciled
    .filter(F.col("pressure_avg_5_prior").between(1002, 1012))
    .select(
        "id",
        "device_code",
        "pressure",
        "status_bits",
        "firmware",
        "pressure_per_status_bits",
        "status_bits_plus_temp_c",
        "temp_c_adjusted",
        "pressure_int",
        "temp_c_avg_5_prior",
        "prev_pressure",
        "row_seq",
        "pressure_dense_rank",
        F.length("device_code").alias("device_code_length"),
    )
)

stats = (
    cleaned
    .groupBy("device_code")
    .agg(
        F.bit_or("status_bits").alias("status_bits_bit_or"),
        F.count(F.when(F.col("device_code") == "DV-01", 1)).alias("dv_01_rows"),
        F.bit_or("status_bits").alias("status_bits_bit_or_2"),
    )
)

result = (
    stats
    .select(
        "device_code",
        "status_bits_bit_or",
        "status_bits_bit_or_2",
        F.when(F.col("device_code").rlike("^(DV-02|DV-03)$"), "PRIORITY").otherwise("GROUP_B").alias("device_code_zone"),
    )
    .orderBy(F.col("status_bits_bit_or_2").asc_nulls_last())
)

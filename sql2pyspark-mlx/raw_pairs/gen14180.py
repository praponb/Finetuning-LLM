from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("status_bits_adjusted").asc_nulls_last())

valid = (
    df_device_readings
    .filter(F.col("temp_c") >= 33)
    .select(
        "id",
        "temp_c",
        "status_bits",
        "firmware",
        F.coalesce("pressure", F.lit(0)).alias("pressure_filled"),
    )
)

valid_device_readings = (
    valid
    .filter(F.col("pressure_filled").between(68, 69))
    .withColumn("status_bits_adjusted", F.round((F.col("status_bits") * 0.9), 2))
)

matched_keys = (
    valid_device_readings
    .select("firmware", "id")
    .exceptAll(
        df_device_readings
        .filter(F.col("status_bits") == 8)
        .select("firmware", "id")
    )
)

verified = (
    valid_device_readings
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.firmware") == F.col("k.firmware")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

valid_device_readings_2 = (
    verified
    .filter(F.col("status_bits").between(11, 12))
    .select(
        "temp_c",
        "status_bits",
        "firmware",
        "pressure_filled",
        "status_bits_adjusted",
        F.trim("firmware").alias("clean_firmware"),
    )
)

metrics = (
    valid_device_readings_2
    .withColumn("status_bits_adjusted_dense_rank", F.dense_rank().over(w))
)

result = (
    metrics
    .groupBy(
        F.when(F.col("status_bits_adjusted") >= 75, "HIGH").otherwise("LOW").alias("status_bits_adjusted_tier"),
        F.when(F.col("pressure_filled") >= 70, "LARGE").otherwise("SMALL").alias("pressure_filled_tier"),
        F.when(F.col("status_bits_adjusted_dense_rank") >= 9, "HIGH").otherwise("LOW").alias("status_bits_adjusted_dense_rank_tier"),
    )
    .agg(
        F.bit_and("status_bits").alias("status_bits_bit_and"),
        F.round(F.avg("status_bits_adjusted_dense_rank"), 2).alias("avg_status_bits_adjusted_dense_rank"),
        F.sum("status_bits").alias("total_status_bits"),
        F.bit_and("status_bits").alias("status_bits_bit_and_2"),
        F.percentile_approx("status_bits_adjusted", [0.25, 0.5, 0.75]).alias("status_bits_adjusted_quartiles"),
        F.avg("pressure_filled").alias("avg_pressure_filled"),
    )
    .orderBy(F.col("status_bits_adjusted_dense_rank_tier").desc())
)

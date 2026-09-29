from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_recent = Window.partitionBy("device_code").orderBy(F.col("temp_c").desc(), "id")
w2 = Window.partitionBy("device_code").orderBy("temp_c", "id")
w3 = Window.partitionBy(F.to_date("reading_ts"), "device_code")
w4 = Window.partitionBy("device_code").orderBy("humidity", "id").rowsBetween(-2, -1)

enriched = (
    df_device_readings
    .select(
        "id",
        "reading_ts",
        "temp_c",
        "pressure",
        "status_bits",
        "firmware",
        F.row_number().over(w_recent).alias("seq_no"),
        F.lag("temp_c", 2, 0).over(w2).alias("prev_temp_c"),
        F.round((F.col("pressure") / F.sum("pressure").over(w3)), 4).alias("pressure_share"),
        F.avg("pressure").over(w4).alias("pressure_avg_2_prior"),
    )
)

valid = (
    enriched
    .filter(F.col("reading_ts") >= F.to_timestamp(F.lit("2025-09-01 07:00:00")))
    .select(
        "id",
        "temp_c",
        "pressure",
        "status_bits",
        "firmware",
        "seq_no",
        "prev_temp_c",
        "pressure_share",
        "pressure_avg_2_prior",
        F.round((F.col("pressure_share") * 1.1), 2).alias("adjusted_pressure_share"),
        (F.col("status_bits") / F.nullif("pressure_share", F.lit(0))).alias("status_bits_per_pressure_share"),
    )
)

valid_device_readings = (
    valid
    .filter(F.col("prev_temp_c") >= 34)
    .select(
        "id",
        "temp_c",
        "pressure",
        "firmware",
        "seq_no",
        "prev_temp_c",
        "pressure_share",
        "pressure_avg_2_prior",
        "adjusted_pressure_share",
        "status_bits_per_pressure_share",
        F.round((F.col("temp_c") * 1.07), 2).alias("temp_c_adjusted"),
    )
)

filtered = (
    valid_device_readings
    .filter(F.col("pressure") < 1022)
    .select(
        "id",
        "pressure",
        "seq_no",
        "pressure_avg_2_prior",
        "adjusted_pressure_share",
        "temp_c_adjusted",
        F.when(F.col("adjusted_pressure_share") >= 87, "High").when(F.col("adjusted_pressure_share") >= 27, "Medium").otherwise("Low").alias("adjusted_pressure_share_level"),
    )
)

valid_device_readings_2 = (
    filtered
    .filter(F.col("seq_no").between(5, 7))
    .select(
        "id",
        "pressure",
        "seq_no",
        "pressure_avg_2_prior",
        "adjusted_pressure_share",
        "temp_c_adjusted",
        "adjusted_pressure_share_level",
        (F.col("adjusted_pressure_share") + F.col("pressure_avg_2_prior")).alias("adjusted_pressure_share_plus_pressure_avg_2_prior"),
    )
)

result = (
    valid_device_readings_2
    .groupBy(
        F.when(F.col("adjusted_pressure_share") >= 64, "LARGE").otherwise("SMALL").alias("adjusted_pressure_share_tier"),
    )
    .agg(
        F.countDistinct("id").alias("distinct_ids"),
        F.round(F.avg("adjusted_pressure_share_plus_pressure_avg_2_prior"), 2).alias("avg_adjusted_pressure_share_plus_pressure_avg_2_prior"),
    )
    .orderBy(
        F.col("avg_adjusted_pressure_share_plus_pressure_avg_2_prior").desc_nulls_first(),
    )
)

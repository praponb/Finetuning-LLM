from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code")
w2 = Window.partitionBy("device_code").orderBy("humidity", "id")
w_recent = Window.partitionBy("device_code").orderBy(F.col("status_bits").desc(), "id")
w4 = Window.partitionBy("row_seq_tier").orderBy(F.col("status_bits_plus_row_seq_p90").desc())

filtered = (
    df_device_readings
    .filter(F.col("status_bits") != 6)
    .withColumn("firmware_group", F.when(F.col("firmware").isin(["1.0.3", "2.0.1"]), "Core").otherwise("Other"))
)

scored = (
    filtered
    .select(
        "id",
        "reading_ts",
        "temp_c",
        "pressure",
        "status_bits",
        "firmware",
        "firmware_group",
        F.round((F.col("temp_c") / F.sum("temp_c").over(w)), 4).alias("temp_c_share"),
        F.ntile(5).over(w2).alias("humidity_bucket"),
        F.row_number().over(w_recent).alias("row_seq"),
    )
)

base = (
    scored
    .filter(F.col("reading_ts") > F.lit("2025-09-01").cast("date"))
    .select(
        "id",
        "temp_c",
        "status_bits",
        "firmware",
        "humidity_bucket",
        "row_seq",
        F.trim("firmware").alias("clean_firmware"),
    )
)

base_device_readings = (
    base
    .filter(F.col("row_seq").isNull())
    .withColumn("firmware_prefix", F.substring("firmware", 1, 3))
)

base_device_readings_2 = (
    base_device_readings
    .filter(F.col("temp_c") >= 24)
    .select(
        "*",
        (F.col("status_bits") + F.col("row_seq")).alias("status_bits_plus_row_seq"),
        F.upper("firmware").alias("firmware_upper"),
    )
)

filtered_device_readings = (
    base_device_readings_2
    .filter(F.col("status_bits_plus_row_seq").isNotNull())
    .select(
        "id",
        "temp_c",
        "status_bits",
        "firmware",
        "humidity_bucket",
        "row_seq",
        "firmware_prefix",
        "status_bits_plus_row_seq",
        "firmware_upper",
        (F.col("status_bits") * F.col("row_seq")).alias("status_bits_times_row_seq"),
    )
)

result = (
    filtered_device_readings
    .groupBy(
        F.when(F.col("row_seq") >= 7, "HIGH").otherwise("LOW").alias("row_seq_tier"),
        F.when(F.col("status_bits") >= 12, "LARGE").otherwise("SMALL").alias("status_bits_tier"),
    )
    .agg(
        F.corr("status_bits", "temp_c").alias("corr_status_bits_temp_c"),
        F.percentile_approx("humidity_bucket", [0.25, 0.5, 0.75]).alias("humidity_bucket_quartiles"),
        F.percentile_approx("status_bits_plus_row_seq", 0.9).alias("status_bits_plus_row_seq_p90"),
    )
    .filter(F.col("corr_status_bits_temp_c") < 1000000)
    .withColumn("status_bits_plus_row_seq_p90_rank", F.rank().over(w4))
    .orderBy(F.col("corr_status_bits_temp_c").desc())
)

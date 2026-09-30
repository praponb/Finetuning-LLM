from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code").orderBy(F.col("status_bits").desc(), "id")

cleaned = (
    df_device_readings
    .filter((F.col("reading_ts") > F.lit("2025-09-01 14:00:00").cast("timestamp")) & F.col("pressure").between(992, 1013))
    .select(
        "id",
        "device_code",
        "temp_c",
        "pressure",
        "status_bits",
        "firmware",
        (F.col("humidity") - F.col("temp_c")).alias("humidity_minus_temp_c"),
    )
)

ranked = (
    cleaned
    .withColumn("rank_in_group", F.row_number().over(w))
    .filter(F.col("rank_in_group") == 1)
    .drop("rank_in_group")
)

filtered = (
    ranked
    .filter(F.col("firmware") != "1.1.0")
    .withColumn("humidity_minus_temp_c_rounded", F.round("humidity_minus_temp_c", 1))
)

result = (
    filtered
    .groupBy(
        F.when(F.col("humidity_minus_temp_c") >= 89, "PREMIUM").otherwise("STANDARD").alias("humidity_minus_temp_c_tier"),
    )
    .agg(
        F.bit_or("status_bits").alias("status_bits_bit_or"),
        F.avg("humidity_minus_temp_c").alias("avg_humidity_minus_temp_c"),
        F.count("*").alias("num_rows"),
    )
    .orderBy(F.col("num_rows").desc_nulls_first())
)

from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code").orderBy(F.col("pressure").desc_nulls_first(), "id")

base = (
    df_device_readings
    .filter(F.col("reading_ts") >= F.lit("2025-09-01").cast("date"))
    .select(
        "id",
        "device_code",
        "temp_c",
        "humidity",
        "pressure",
        "status_bits",
        "firmware",
        F.substring("device_code", 1, 3).alias("device_code_prefix"),
    )
)

ranked = (
    base
    .withColumn("latest_rank", F.row_number().over(w))
    .filter(F.col("latest_rank") == 1)
)

base_device_readings = (
    ranked
    .filter(F.col("latest_rank") == 1)
    .select(
        "id",
        "device_code",
        "status_bits",
        "device_code_prefix",
        "latest_rank",
        F.initcap(F.trim("device_code")).alias("device_code_title"),
    )
)

base_device_readings_2 = (
    base_device_readings
    .filter(F.col("device_code") == "DV-03")
    .withColumn("least_latest_rank_status_bits", F.least("latest_rank", "status_bits"))
)

filtered = (
    base_device_readings_2
    .filter(F.col("status_bits") > 5)
    .withColumn("least_latest_rank_status_bits_level", F.when(F.col("least_latest_rank_status_bits") >= 61, "High").when(F.col("least_latest_rank_status_bits") >= 38, "Medium").otherwise("Low"))
    .withColumn("status_bits_whole", F.ceil("status_bits"))
)

result = (
    filtered
    .select(
        "id",
        "device_code",
        "status_bits",
        "device_code_prefix",
        "device_code_title",
        "least_latest_rank_status_bits_level",
        F.when(F.col("device_code").rlike("^(DV-01|DV-03)$"), "OTHER_ZONE").when(F.col("device_code").rlike("^(DV-02|DV-04)$"), "GROUP_A").otherwise("GROUP_B").alias("device_code_zone"),
    )
    .orderBy(F.col("device_code").asc_nulls_first())
)

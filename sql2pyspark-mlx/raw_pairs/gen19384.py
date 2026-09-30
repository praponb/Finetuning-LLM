from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code").orderBy("status_bits", "id")

cleaned = (
    df_device_readings
    .filter(F.col("reading_ts") >= F.lit("2025-09-01 03:00:00").cast("timestamp"))
    .select(
        "id",
        "device_code",
        "reading_ts",
        "humidity",
        "status_bits",
        "firmware",
        (F.col("pressure") * F.col("temp_c")).alias("pressure_times_temp_c"),
        F.concat("firmware", F.lit(" "), "device_code").alias("firmware_device_code"),
    )
)

deduped = (
    cleaned
    .withColumn("latest_rank", F.row_number().over(w))
    .filter(F.col("latest_rank") == 1)
    .select("id", "device_code", "firmware", "firmware_device_code")
)

filtered = (
    deduped
    .filter(F.col("firmware") == "1.0.3")
    .select(
        "*",
        F.when(F.col("device_code").isin(["DV-03", "DV-02"]), "Priority").otherwise("Other").alias("device_code_group"),
        F.concat_ws(",", "firmware", "device_code").alias("firmware_device_code_key"),
    )
)

cleaned_device_readings = (
    filtered
    .filter(F.col("firmware").isin(["1.0.3", "1.1.0", "2.0.1"]))
    .withColumn("device_code_group_2", F.when(F.col("device_code").isin(["DV-04", "DV-01"]), "Tier 1").otherwise("Other"))
)

base = (
    cleaned_device_readings
    .filter(F.col("device_code").isin(["DV-01", "DV-02", "DV-03"]))
    .withColumn("device_code_group_3", F.when(F.col("device_code").isin(["DV-01", "DV-02"]), "Group A").otherwise("Other"))
    .withColumn("device_code_title", F.initcap(F.trim("device_code")))
)

filtered_device_readings = (
    base
    .filter(F.col("firmware").isin(["1.1.0", "1.0.3"]))
    .select(
        "id",
        "device_code",
        "firmware",
        "firmware_device_code",
        "device_code_group",
        F.substring("firmware", 1, 3).alias("firmware_prefix"),
        F.trim("device_code").alias("clean_device_code"),
    )
)

result = (
    filtered_device_readings
    .groupBy("device_code")
    .agg(
        F.size(F.collect_set("device_code")).alias("num_device_codes"),
        F.count_if(F.col("device_code") == "DV-02").alias("dv_02_events"),
        F.count(F.when(F.col("device_code") == "DV-01", 1)).alias("dv_01_count"),
        F.size(F.collect_set("device_code")).alias("num_device_codes_2"),
        F.count(F.when(F.col("device_code") == "DV-04", 1)).alias("dv_04_rows"),
        F.count_if(F.col("device_code") == "DV-02").alias("dv_02_events_2"),
    )
    .filter(F.col("dv_02_events") < 1000)
)

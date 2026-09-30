from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code").orderBy("status_bits", "id")
w2 = Window.partitionBy("device_code")
w3 = Window.partitionBy("device_code").orderBy(F.col("prev_humidity_minus_pressure_rounded").asc_nulls_first(), "id")
w4 = Window.partitionBy("device_code").orderBy(F.col("humidity_minus_pressure_plus_humidity").desc_nulls_last(), "id")

filtered = (
    df_device_readings
    .filter(F.col("reading_ts") > F.to_date(F.lit("2025-09-01")))
    .select(
        "id",
        "device_code",
        "temp_c",
        "humidity",
        "status_bits",
        "firmware",
        (F.col("humidity") - F.col("pressure")).alias("humidity_minus_pressure"),
    )
)

base = (
    filtered
    .filter(~F.col("firmware").isin(["2.0.1", "1.1.0"]))
    .select(
        "id",
        "device_code",
        "temp_c",
        "humidity",
        "status_bits",
        "humidity_minus_pressure",
        F.initcap(F.trim("device_code")).alias("device_code_title"),
        (F.col("humidity_minus_pressure") + F.col("humidity")).alias("humidity_minus_pressure_plus_humidity"),
    )
)

base_device_readings = (
    base
    .filter(F.col("temp_c") >= 29)
    .select(
        "id",
        "device_code",
        "temp_c",
        "status_bits",
        "humidity_minus_pressure",
        "humidity_minus_pressure_plus_humidity",
        F.round("humidity_minus_pressure", 2).alias("humidity_minus_pressure_rounded"),
    )
)

enriched = (
    base_device_readings
    .filter(F.col("humidity_minus_pressure_rounded") < 51)
    .select(
        "*",
        F.lead("humidity_minus_pressure_plus_humidity").over(w).alias("next_humidity_minus_pressure_plus_humidity"),
        F.lead("humidity_minus_pressure_rounded").over(w).alias("next_humidity_minus_pressure_rounded"),
        F.lag("humidity_minus_pressure_rounded").over(w).alias("prev_humidity_minus_pressure_rounded"),
        F.row_number().over(w).alias("event_seq"),
        F.max("humidity_minus_pressure").over(w2).alias("device_code_max_humidity_minus_pressure"),
    )
)

latest = (
    enriched
    .withColumn("dedup_rank", F.row_number().over(w3))
    .filter(F.col("dedup_rank") == 1)
    .drop("dedup_rank")
)

deduped = (
    latest
    .withColumn("rank_in_group", F.row_number().over(w4))
    .filter(F.col("rank_in_group") == 1)
)

result = (
    deduped
    .select(
        "id",
        "temp_c",
        "humidity_minus_pressure_plus_humidity",
        "humidity_minus_pressure_rounded",
        "next_humidity_minus_pressure_plus_humidity",
        "event_seq",
        "device_code_max_humidity_minus_pressure",
        "rank_in_group",
    )
)

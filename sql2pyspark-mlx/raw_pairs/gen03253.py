from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("pressure", "id").rowsBetween(-6, Window.currentRow)
w2 = Window.partitionBy("device_code").orderBy(F.col("humidity").asc_nulls_first())
w3 = Window.partitionBy("device_code").orderBy(F.col("pressure").desc(), "id")
w4 = Window.partitionBy("device_code").orderBy("status_bits", "id")
w5 = Window.partitionBy("device_code").orderBy(F.col("humidity_rank").desc_nulls_last(), "id")
w6 = Window.partitionBy("device_code").orderBy(F.col("temp_c").desc(), "id")

filtered = (
    df_device_readings
    .filter(F.col("reading_ts") > F.lit("2025-09-01").cast("date"))
    .select(
        "id",
        "device_code",
        "temp_c",
        "humidity",
        "pressure",
        "status_bits",
        "firmware",
        F.concat("device_code", F.lit(" "), "firmware").alias("device_code_firmware"),
        F.concat_ws(",", "firmware", "device_code").alias("firmware_device_code_key"),
    )
)

enriched = (
    filtered
    .filter(F.col("humidity") <= 50)
    .select(
        "*",
        F.avg("humidity").over(w).alias("humidity_avg_6"),
        F.rank().over(w2).alias("humidity_rank"),
        F.row_number().over(w3).alias("event_seq"),
    )
)

latest = (
    enriched
    .withColumn("rank_in_group", F.row_number().over(w4))
    .filter(F.col("rank_in_group") == 1)
)

deduped = (
    latest
    .withColumn("rn", F.row_number().over(w5))
    .filter(F.col("rn") == 1)
)

latest_device_readings = (
    deduped
    .withColumn("latest_rank", F.row_number().over(w6))
    .filter(F.col("latest_rank") == 1)
    .select(
        "id",
        "humidity",
        "pressure",
        "firmware",
        "firmware_device_code_key",
        "humidity_avg_6",
        "humidity_rank",
        "rank_in_group",
        "rn",
    )
)

result = (
    latest_device_readings
    .groupBy(
        F.when(F.col("humidity") >= 75, "HIGH").otherwise("LOW").alias("humidity_tier"),
        F.when(F.col("pressure") >= 1020, "LARGE").otherwise("SMALL").alias("pressure_tier"),
        F.when(F.col("humidity_rank") >= 7, "LARGE").otherwise("SMALL").alias("humidity_rank_tier"),
    )
    .agg(
        F.max("humidity").alias("max_humidity"),
        F.count("*").alias("cnt"),
    )
    .orderBy("pressure_tier")
)

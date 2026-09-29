from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code").orderBy(F.col("status_bits_str").asc_nulls_first(), "id")
w_hist = Window.partitionBy("device_code").orderBy("status_bits_str", "id")
w3 = Window.partitionBy(F.month("reading_ts"), "device_code").orderBy(F.col("reading_ts").desc(), "id")
w4 = Window.partitionBy("device_code").orderBy(F.col("status_bits").desc(), "id")
w5 = Window.partitionBy("device_code").orderBy("prev_humidity", "id")

valid = (
    df_device_readings
    .filter((F.col("reading_ts") > F.lit("2025-09-01").cast("date")) & (F.col("device_code") != "DV-04"))
    .select(
        "id",
        "device_code",
        "reading_ts",
        "temp_c",
        "humidity",
        "pressure",
        "status_bits",
        "firmware",
        F.col("status_bits").cast("double").alias("status_bits_str"),
    )
)

deduped = (
    valid
    .withColumn("dedup_rank", F.row_number().over(w))
    .filter(F.col("dedup_rank") == 1)
    .drop("dedup_rank")
)

enriched = (
    deduped
    .select(
        "id",
        "device_code",
        "temp_c",
        "pressure",
        "status_bits",
        "firmware",
        "status_bits_str",
        F.lead("temp_c").over(w_hist).alias("next_temp_c"),
        F.lag("humidity").over(w_hist).alias("prev_humidity"),
        F.lead("humidity").over(w_hist).alias("next_humidity"),
        F.row_number().over(w_hist).alias("event_seq"),
        F.row_number().over(w3).alias("seq_no"),
    )
)

latest = (
    enriched
    .withColumn("rn", F.row_number().over(w4))
    .filter(F.col("rn") == 1)
)

ranked = (
    latest
    .withColumn("row_num", F.row_number().over(w5))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

result = (
    ranked
    .withColumn("prev_humidity_tier", F.when(F.col("prev_humidity") >= 81, "LARGE").otherwise("SMALL"))
    .groupingSets(
        [["prev_humidity_tier", "device_code"], ["prev_humidity_tier"]],
        "prev_humidity_tier",
        "device_code",
    )
    .agg(
        F.countDistinct("firmware").alias("distinct_firmwares"),
        F.count("*").alias("row_count"),
        F.count(F.when(F.col("device_code") == "DV-02", 1)).alias("dv_02_rows"),
    )
    .orderBy(F.col("distinct_firmwares").asc_nulls_last())
)

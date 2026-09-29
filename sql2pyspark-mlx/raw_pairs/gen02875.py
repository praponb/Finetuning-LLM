from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code")
w2 = Window.partitionBy("device_code").orderBy(F.col("reading_ts").desc(), "id")
w3 = Window.orderBy("temp_c", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_running = Window.partitionBy("device_code").orderBy("humidity", "id").rowsBetween(-5, Window.currentRow)
w5 = Window.partitionBy("device_code").orderBy(F.col("humidity_share").desc(), "id")

valid = (
    df_device_readings
    .filter(F.col("firmware") == "1.0.3")
    .withColumn("device_code_firmware", F.concat("device_code", F.lit(" - "), "firmware"))
)

with_windows = (
    valid
    .select(
        "id",
        "device_code",
        "temp_c",
        "humidity",
        "status_bits",
        "firmware",
        F.round((F.col("humidity") / F.sum("humidity").over(w)), 4).alias("humidity_share"),
        F.round((F.col("humidity") / F.sum("humidity").over(w)), 4).alias("humidity_share_2"),
        F.row_number().over(w2).alias("event_seq"),
    )
)

filtered = (
    with_windows
    .filter(F.col("humidity") <= 67)
    .select("*", F.col("temp_c").cast("int").alias("temp_c_int"))
)

metrics = (
    filtered
    .select(
        "*",
        F.first("event_seq", ignorenulls=True).over(w3).alias("first_known_event_seq"),
        F.avg("humidity_share_2").over(w_running).alias("humidity_share_2_avg_5"),
        F.row_number().over(w5).alias("event_seq_2"),
        F.avg("humidity_share_2").over(w).alias("device_code_avg_humidity_share_2"),
    )
)

totals = (
    metrics
    .withColumn("status_bits_tier", F.when(F.col("status_bits") >= 11, "HIGH").otherwise("LOW"))
    .withColumn("first_known_event_seq_tier", F.when(F.col("first_known_event_seq") >= 8, "PREMIUM").otherwise("STANDARD"))
    .groupingSets(
        [["status_bits_tier", "first_known_event_seq_tier"], ["status_bits_tier"], []],
        "status_bits_tier",
        "first_known_event_seq_tier",
    )
    .agg(
        F.bit_or("status_bits").alias("status_bits_bit_or"),
        F.round(F.sum("humidity_share") / F.nullif(F.count(F.when(F.col("device_code") == "DV-03", 1)), F.lit(0)), 2).alias("humidity_share_per_dv_03"),
        F.percentile_approx("first_known_event_seq", 0.95).alias("first_known_event_seq_p95"),
    )
    .filter(F.col("status_bits_bit_or") > 1)
)

result = (
    totals
    .select(
        "first_known_event_seq_tier",
        "status_bits_bit_or",
        "humidity_share_per_dv_03",
    )
    .orderBy(F.col("status_bits_bit_or").desc_nulls_first(), "humidity_share_per_dv_03")
)

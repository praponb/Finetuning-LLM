from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_part = Window.partitionBy("device_code").orderBy("temp_c", "id")
w2 = Window.partitionBy(F.month("reading_ts"), "device_code").orderBy("reading_ts", "id").rowsBetween(-3, -1)
w3 = Window.partitionBy("device_code").orderBy(F.col("status_bits").desc(), "id")
w4 = Window.orderBy("prev_temp_c", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("device_code").orderBy(F.col("first_known_humidity_avg_3_prior_minus_humidity").asc_nulls_last(), "id")

base = (
    df_device_readings
    .filter(~F.col("firmware").isin(["2.0.1", "1.0.3"]))
    .withColumn("is_1_0_3", F.when(F.col("firmware") == "1.0.3", 1).otherwise(0))
)

metrics = (
    base
    .select(
        "id",
        "device_code",
        "humidity",
        "pressure",
        "status_bits",
        "firmware",
        "is_1_0_3",
        F.lag("humidity").over(w_part).alias("prev_humidity"),
        F.lag("temp_c").over(w_part).alias("prev_temp_c"),
        F.lead("temp_c").over(w_part).alias("next_temp_c"),
        F.row_number().over(w_part).alias("seq_no"),
        F.avg("humidity").over(w2).alias("humidity_avg_3_prior"),
    )
)

metrics_device_readings = (
    metrics
    .select(
        "id",
        "device_code",
        "humidity",
        "pressure",
        "status_bits",
        "firmware",
        "prev_humidity",
        "next_temp_c",
        "seq_no",
        "humidity_avg_3_prior",
        F.ntile(5).over(w3).alias("status_bits_bucket"),
        F.first("humidity_avg_3_prior", ignorenulls=True).over(w4).alias("first_known_humidity_avg_3_prior"),
    )
)

valid = (
    metrics_device_readings
    .filter(F.col("seq_no") <= 6)
    .select(
        "id",
        "device_code",
        "humidity",
        "pressure",
        "status_bits",
        "next_temp_c",
        "seq_no",
        "humidity_avg_3_prior",
        "first_known_humidity_avg_3_prior",
        (F.col("humidity_avg_3_prior") * F.col("status_bits_bucket")).alias("humidity_avg_3_prior_times_status_bits_bucket"),
        (F.col("first_known_humidity_avg_3_prior") - F.col("humidity")).alias("first_known_humidity_avg_3_prior_minus_humidity"),
    )
)

latest = (
    valid
    .withColumn("rank_in_group", F.row_number().over(w5))
    .filter(F.col("rank_in_group") == 1)
)

result = (
    latest
    .select(
        "id",
        "device_code",
        "humidity",
        "next_temp_c",
        "seq_no",
        "humidity_avg_3_prior",
        "first_known_humidity_avg_3_prior",
        "humidity_avg_3_prior_times_status_bits_bucket",
        "first_known_humidity_avg_3_prior_minus_humidity",
        F.when(F.col("device_code").rlike("^(DV-01|DV-02)$"), "PRIORITY").otherwise("CORE").alias("device_code_zone"),
    )
    .orderBy(
        "first_known_humidity_avg_3_prior",
        F.col("first_known_humidity_avg_3_prior_minus_humidity").desc(),
    )
)

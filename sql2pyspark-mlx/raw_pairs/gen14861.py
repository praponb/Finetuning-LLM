from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code").orderBy(F.col("pressure").desc(), "id")
w2 = Window.partitionBy("device_code").orderBy("status_bits", "id").rowsBetween(-2, Window.currentRow)
w_part = Window.orderBy("status_bits_num", "id")
w_hist = Window.orderBy("next_last3_max_status_bits", "id").rowsBetween(-2, Window.currentRow)
w5 = Window.orderBy(F.col("next_temp_c").desc(), "id")

valid = (
    df_device_readings
    .filter(F.col("reading_ts") >= F.lit("2025-09-01").cast("date"))
    .withColumn("status_bits_num", F.col("status_bits").cast("double"))
)

deduped = (
    valid
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

metrics = (
    deduped
    .select(
        "id",
        "temp_c",
        "firmware",
        "status_bits_num",
        F.min("status_bits").over(w2).alias("last3_min_status_bits"),
        F.sum("status_bits").over(w2).alias("last3_sum_status_bits"),
        F.max("status_bits").over(w2).alias("last3_max_status_bits"),
    )
)

metrics_device_readings = (
    metrics
    .select(
        "*",
        F.lag("temp_c").over(w_part).alias("prev_temp_c"),
        F.lead("temp_c").over(w_part).alias("next_temp_c"),
        F.lead("last3_max_status_bits").over(w_part).alias("next_last3_max_status_bits"),
        F.row_number().over(w_part).alias("event_seq"),
    )
)

base = (
    metrics_device_readings
    .filter(F.col("firmware") == "2.0.1")
    .withColumn("adjusted_next_temp_c", F.round((F.col("next_temp_c") * 1.15), 2))
)

result = (
    base
    .select(
        "id",
        "firmware",
        "status_bits_num",
        "next_temp_c",
        "next_last3_max_status_bits",
        "adjusted_next_temp_c",
        F.min("temp_c").over(w_hist).alias("last3_min_temp_c"),
        F.max("temp_c").over(w_hist).alias("last3_max_temp_c"),
        F.count("temp_c").over(w_hist).alias("last3_count_temp_c"),
        F.row_number().over(w5).alias("row_seq"),
    )
)

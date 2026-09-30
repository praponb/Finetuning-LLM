from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("humidity_per_temp_c").desc_nulls_first())
w_recent = Window.orderBy("temp_c", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_recent_2 = Window.orderBy("status_bits", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.orderBy(F.col("reading_ts_period").cast("bigint")).rangeBetween(-7776000, Window.currentRow)
w5 = Window.orderBy("humidity_per_temp_c", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.orderBy(F.col("humidity_per_temp_c_percent_rank").desc())
w7 = Window.orderBy("first_known_humidity_per_temp_c", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w8 = Window.orderBy("pressure", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w9 = Window.orderBy(F.col("temp_c_count_90d").asc_nulls_first())
w10 = Window.orderBy("reading_ts_period", "id").rowsBetween(-3, -1)
w11 = Window.orderBy(F.col("temp_c_count_90d").desc(), "id")
w12 = Window.orderBy(F.col("first_known_pressure").asc_nulls_first())
w13 = Window.orderBy("temp_c", "id")
w14 = Window.orderBy("first_known_pressure_rank", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w15 = Window.orderBy("row_seq", "id")
w16 = Window.orderBy("last_known_humidity_per_temp_c", "id")
w17 = Window.orderBy("first_known_humidity_per_temp_c_2", "id")
w18 = Window.orderBy("first_known_pressure", "id")
w19 = Window.orderBy("row_seq", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_running = Window.orderBy("pressure", "id").rowsBetween(-3, Window.currentRow)
w_part = Window.orderBy("reading_ts_period", "id")

valid = (
    df_device_readings
    .filter(F.col("temp_c").between(29, 38))
    .select(
        "id",
        "reading_ts",
        "temp_c",
        "pressure",
        "status_bits",
        F.round((F.col("humidity") / F.col("temp_c")), 2).alias("humidity_per_temp_c"),
    )
)

filtered = (
    valid
    .filter(F.col("temp_c") < 26)
    .select(
        "*",
        F.date_trunc("month", "reading_ts").alias("reading_ts_period"),
        (F.col("humidity_per_temp_c") - F.col("pressure")).alias("humidity_per_temp_c_minus_pressure"),
    )
)

metrics = (
    filtered
    .withColumn("humidity_per_temp_c_percent_rank", F.percent_rank().over(w))
    .withColumn("last_known_humidity_per_temp_c", F.last("humidity_per_temp_c", ignorenulls=True).over(w_recent))
    .withColumn("first_known_humidity_per_temp_c", F.first("humidity_per_temp_c", ignorenulls=True).over(w_recent_2))
)

enriched = (
    metrics
    .withColumn("temp_c_count_90d", F.count("temp_c").over(w4))
    .withColumn("first_known_last_known_humidity_per_temp_c", F.first("last_known_humidity_per_temp_c", ignorenulls=True).over(w5))
    .withColumn("humidity_per_temp_c_percent_rank_cume_dist", F.cume_dist().over(w6))
    .withColumn("first_known_pressure", F.first("pressure", ignorenulls=True).over(w7))
    .withColumn("first_known_humidity_per_temp_c_2", F.first("humidity_per_temp_c", ignorenulls=True).over(w8))
)

metrics_device_readings = (
    enriched
    .select(
        "*",
        F.percent_rank().over(w9).alias("temp_c_count_90d_percent_rank"),
        F.avg("first_known_humidity_per_temp_c_2").over(w10).alias("first_known_humidity_per_temp_c_2_avg_3_prior"),
        F.row_number().over(w11).alias("row_seq"),
        F.rank().over(w12).alias("first_known_pressure_rank"),
        F.lag("temp_c", 1).over(w13).alias("prev_temp_c"),
    )
)

enriched_device_readings = (
    metrics_device_readings
    .select(
        "*",
        F.sum("first_known_pressure_rank").over(w14).alias("cumulative_first_known_pressure_rank"),
        F.row_number().over(w15).alias("seq_no"),
        F.lead("temp_c", 1).over(w16).alias("next_temp_c"),
    )
)

with_windows = (
    enriched_device_readings
    .select(
        "*",
        F.lag("humidity_per_temp_c_minus_pressure", 1).over(w17).alias("prev_humidity_per_temp_c_minus_pressure"),
        F.row_number().over(w18).alias("seq_no_2"),
        F.sum("first_known_pressure_rank").over(w19).alias("running_first_known_pressure_rank"),
    )
)

result = (
    with_windows
    .withColumn("humidity_per_temp_c_minus_pressure_avg_3", F.avg("humidity_per_temp_c_minus_pressure").over(w_running))
    .withColumn("next_temp_c_2", F.lead("temp_c", 2).over(w_part))
)

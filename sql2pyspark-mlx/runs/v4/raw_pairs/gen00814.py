from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("humidity_times_temp_c").desc(), "id")
w2 = Window.orderBy("temp_c", "id")
w_hist = Window.orderBy(F.col("status_bits").desc())
w4 = Window.orderBy("humidity_times_temp_c", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.orderBy(F.col("status_bits").desc(), "id")
w6 = Window.orderBy("status_bits_cume_dist", "id")
w7 = Window.partitionBy("row_seq_tier").orderBy(F.col("avg_next_pressure").desc())

valid = (
    df_device_readings
    .filter(F.col("device_code") == "DV-01")
    .select(
        "id",
        "temp_c",
        "pressure",
        "status_bits",
        "firmware",
        (F.col("humidity") * F.col("temp_c")).alias("humidity_times_temp_c"),
        F.upper("firmware").alias("firmware_upper"),
    )
)

scored = (
    valid
    .select(
        "*",
        F.ntile(4).over(w).alias("humidity_times_temp_c_bucket"),
        F.lead("pressure", 2).over(w2).alias("next_pressure"),
        F.cume_dist().over(w_hist).alias("status_bits_cume_dist"),
    )
)

metrics = (
    scored
    .select(
        "*",
        F.first("status_bits_cume_dist", ignorenulls=True).over(w4).alias("first_known_status_bits_cume_dist"),
        F.row_number().over(w5).alias("event_seq"),
        F.row_number().over(w6).alias("row_seq"),
    )
)

result = (
    metrics
    .groupBy(
        F.when(F.col("row_seq") >= 8, "LARGE").otherwise("SMALL").alias("row_seq_tier"),
        F.when(F.col("next_pressure") >= 1033, "LARGE").otherwise("SMALL").alias("next_pressure_tier"),
    )
    .agg(
        F.count("*").alias("event_count"),
        F.max("first_known_status_bits_cume_dist").alias("max_first_known_status_bits_cume_dist"),
        F.round(F.avg("next_pressure"), 2).alias("avg_next_pressure"),
        F.percentile_approx("humidity_times_temp_c", 0.5).alias("humidity_times_temp_c_p50"),
        F.percentile_approx("status_bits", [0.25, 0.5, 0.75]).alias("status_bits_quartiles"),
    )
    .withColumn("avg_next_pressure_dense_rank", F.dense_rank().over(w7))
    .orderBy(F.col("next_pressure_tier").desc())
)

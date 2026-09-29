from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code").orderBy("reading_ts", "id")
w2 = Window.partitionBy("device_code").orderBy("reading_ts", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("device_code").orderBy("status_bits", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("device_code").orderBy(F.col("temp_c").desc_nulls_first())
w5 = Window.partitionBy("device_code").orderBy("running_count_island_id", "id").rowsBetween(-5, -1)
w6 = Window.partitionBy("device_code").orderBy(F.col("running_sum_island_id").asc_nulls_last())
w7 = Window.partitionBy("device_code")
w8 = Window.partitionBy("device_code").orderBy("row_num", "id")

valid = (
    df_device_readings
    .filter(F.col("temp_c").isNull())
    .withColumn("status_bits_num", F.col("status_bits").cast("double"))
    .withColumn("reading_ts_day", F.col("reading_ts").cast("date"))
)

gaps = (
    valid
    .withColumn("session_start_flag", F.when(F.lag("reading_ts").over(w).isNull() | (F.col("reading_ts") > F.lag("reading_ts").over(w) + F.expr("INTERVAL 30 MINUTES")), 1).otherwise(0))
)

sessions_device_readings = (
    gaps
    .select(
        "id",
        "device_code",
        "reading_ts",
        "temp_c",
        "status_bits",
        "status_bits_num",
        F.sum("session_start_flag").over(w2).alias("island_id"),
    )
)

scored = (
    sessions_device_readings
    .withColumn("running_min_island_id", F.min("island_id").over(w3))
    .withColumn("running_count_island_id", F.count("island_id").over(w3))
    .withColumn("running_sum_island_id", F.sum("island_id").over(w3))
)

latest = (
    scored
    .withColumn("row_num", F.rank().over(w4))
    .filter(F.col("row_num") == 1)
)

metrics = (
    latest
    .filter(F.col("island_id") > 3)
    .select(
        "*",
        F.avg("status_bits").over(w5).alias("status_bits_avg_5_prior"),
        F.rank().over(w6).alias("running_sum_island_id_rank"),
    )
)

cleaned = (
    metrics
    .filter(F.col("reading_ts") >= F.lit("2025-09-01").cast("date"))
    .withColumn("status_bits_avg_5_prior_scaled", F.round((F.col("status_bits_avg_5_prior") * 0.9), 2))
)

metrics_device_readings = (
    cleaned
    .select(
        "*",
        F.min("running_sum_island_id_rank").over(w7).alias("device_code_min_running_sum_island_id_rank"),
        F.avg("temp_c").over(w7).alias("device_code_avg_temp_c"),
        F.lead("status_bits_avg_5_prior", 1).over(w8).alias("next_status_bits_avg_5_prior"),
    )
)

result = (
    metrics_device_readings
    .groupBy(
        F.when(F.col("temp_c") >= 38.2, "PREMIUM").otherwise("STANDARD").alias("temp_c_tier"),
    )
    .agg(
        F.bit_and("status_bits").alias("status_bits_bit_and"),
        F.count(F.when(F.col("device_code") == "DV-04", 1)).alias("dv_04_count"),
        F.count(F.when(F.col("device_code") == "DV-04", 1)).alias("dv_04_rows"),
        F.round(F.sum("status_bits") / F.nullif(F.count(F.when(F.col("device_code") == "DV-03", 1)), F.lit(0)), 2).alias("status_bits_per_dv_03"),
        F.percentile_approx("device_code_avg_temp_c", [0.25, 0.5, 0.75]).alias("device_code_avg_temp_c_quartiles"),
        F.size(F.collect_set("device_code")).alias("num_device_codes"),
    )
)

from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code").orderBy("status_bits")
w2 = Window.partitionBy("device_code").orderBy("rn", "id").rowsBetween(-5, Window.currentRow)
w3 = Window.orderBy("status_bits", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("device_code").orderBy(F.col("temp_c").desc_nulls_last())
w_ordered = Window.partitionBy("device_code")
w6 = Window.partitionBy("device_code").orderBy("status_bits", "id")
w7 = Window.partitionBy("device_code").orderBy("rn", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

latest = (
    df_device_readings
    .withColumn("rn", F.rank().over(w))
    .filter(F.col("rn") == 1)
)

scored = (
    latest
    .select(
        "id",
        "device_code",
        "humidity",
        "pressure",
        "status_bits",
        "firmware",
        "rn",
        F.avg("humidity").over(w2).alias("humidity_avg_5"),
        F.sum("temp_c").over(w3).alias("cumulative_temp_c"),
        F.rank().over(w4).alias("temp_c_rank"),
        F.count("humidity").over(w_ordered).alias("device_code_count_humidity"),
    )
)

base = (
    scored
    .filter(F.col("temp_c_rank") == 6)
    .select(
        "id",
        "device_code",
        "pressure",
        "status_bits",
        "firmware",
        "rn",
        "humidity_avg_5",
        "cumulative_temp_c",
        F.when(F.col("firmware").isin(["1.1.0", "1.0.3"]), "Group A").otherwise("Other").alias("firmware_group"),
    )
)

latest_device_readings = (
    base
    .withColumn("row_num", F.row_number().over(w6))
    .filter(F.col("row_num") == 1)
    .select("id", "device_code", "pressure", "firmware", "rn", "cumulative_temp_c")
)

scored_device_readings = (
    latest_device_readings
    .filter(F.col("firmware").isin(["1.1.0", "2.0.1"]))
    .select(
        "id",
        "pressure",
        "cumulative_temp_c",
        F.count("cumulative_temp_c").over(w7).alias("running_count_cumulative_temp_c"),
        F.sum("cumulative_temp_c").over(w7).alias("running_sum_cumulative_temp_c"),
        F.min("cumulative_temp_c").over(w7).alias("running_min_cumulative_temp_c"),
    )
)

matched_keys = (
    scored_device_readings
    .select("id")
    .subtract(
        df_device_readings
        .filter(F.col("device_code").isin(["DV-02", "DV-01", "DV-03"]))
        .select("id")
    )
)

verified = (
    scored_device_readings
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

totals = (
    verified
    .groupBy(
        F.when(F.col("running_count_cumulative_temp_c") >= 15, "PREMIUM").otherwise("STANDARD").alias("running_count_cumulative_temp_c_tier"),
    )
    .agg(
        F.max("running_count_cumulative_temp_c").alias("max_running_count_cumulative_temp_c"),
        F.percentile_approx("running_min_cumulative_temp_c", [0.25, 0.5, 0.75]).alias("running_min_cumulative_temp_c_quartiles"),
    )
)

result = (
    totals
    .select(
        "running_count_cumulative_temp_c_tier",
        "max_running_count_cumulative_temp_c",
    )
    .orderBy(
        F.col("running_count_cumulative_temp_c_tier").asc_nulls_last(),
        "max_running_count_cumulative_temp_c",
    )
)

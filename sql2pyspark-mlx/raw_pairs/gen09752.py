from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code")
w2 = Window.partitionBy(F.date_trunc("week", "reading_ts"), "device_code")
w3 = Window.partitionBy("device_code").orderBy(F.col("temp_c").desc())
w4 = Window.partitionBy(F.date_trunc("WEEK", "reading_ts"))
w5 = Window.partitionBy("device_code").orderBy(F.col("reading_ts").cast("bigint")).rangeBetween(-7200, Window.currentRow)
w_running = Window.orderBy(F.col("status_bits").desc())

base = (
    df_device_readings
    .filter(F.col("reading_ts") >= F.lit("2025-09-01 02:00:00").cast("timestamp"))
    .select("*", F.col("status_bits").cast("string").alias("status_bits_num"))
)

filtered = (
    base
    .filter(F.col("pressure") < 994)
    .select(
        "id",
        "device_code",
        "reading_ts",
        "temp_c",
        "humidity",
        "pressure",
        "status_bits",
        "firmware",
        F.substring("firmware", 1, 4).alias("firmware_prefix"),
    )
)

matched_keys = (
    filtered
    .select("device_code")
    .intersectAll(
        df_device_readings
        .filter(F.col("device_code").isin(["DV-04", "DV-02"]))
        .select("device_code")
    )
)

kept_rows = (
    filtered
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.device_code") == F.col("k.device_code")), "inner")
    .select("s.*")
)

enriched = (
    kept_rows
    .withColumn("device_code_max_temp_c", F.max("temp_c").over(w))
    .withColumn("reading_ts_period_max_pressure", F.max("pressure").over(w2))
    .withColumn("temp_c_percent_rank", F.percent_rank().over(w3))
)

metrics = (
    enriched
    .filter(F.col("temp_c") <= 25)
    .withColumn("reading_ts_period_count_temp_c_percent_rank", F.count("temp_c_percent_rank").over(w4))
    .withColumn("status_bits_count_2h", F.count("status_bits").over(w5))
    .withColumn("device_code_sum_pressure", F.sum("pressure").over(w))
    .withColumn("device_code_count_reading_ts_period_max_pressure", F.count("reading_ts_period_max_pressure").over(w))
    .withColumn("status_bits_cume_dist", F.cume_dist().over(w_running))
)

base_device_readings = (
    metrics
    .filter((F.col("reading_ts") > F.lit("2025-09-01").cast("date")) & F.col("device_code_count_reading_ts_period_max_pressure").between(8, 15))
    .withColumn("years_since_reading_ts", (F.datediff(F.current_date(), "reading_ts") / 365.25))
)

result = (
    base_device_readings
    .groupBy(F.date_trunc("hour", "reading_ts").alias("hour_window"))
    .agg(
        F.sum(F.when(F.col("device_code") == "DV-03", F.col("humidity")).otherwise(0)).alias("dv_03_humidity"),
        F.count("*").alias("cnt"),
        F.countDistinct("firmware").alias("distinct_firmwares"),
    )
)

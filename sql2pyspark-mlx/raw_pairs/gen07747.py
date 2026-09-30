from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_running = Window.partitionBy("device_code").orderBy("humidity", "id").rowsBetween(-2, Window.currentRow)
w_grp = Window.partitionBy("device_code").orderBy("status_bits", "id")
w3 = Window.partitionBy("device_code").orderBy("reading_ts", "id")
w4 = Window.partitionBy("device_code").orderBy("reading_ts", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("device_code").orderBy(F.col("session_no").desc(), "id")

valid = (
    df_device_readings
    .filter(F.col("pressure").isNull())
    .select(
        "id",
        "device_code",
        "reading_ts",
        "temp_c",
        "humidity",
        "status_bits",
        "firmware",
        F.trim("firmware").alias("clean_firmware"),
        F.ceil("temp_c").alias("temp_c_whole"),
    )
)

enriched = (
    valid
    .select(
        "id",
        "device_code",
        "reading_ts",
        "temp_c",
        "status_bits",
        "firmware",
        "temp_c_whole",
        F.min("temp_c_whole").over(w_running).alias("last3_min_temp_c_whole"),
        F.max("temp_c_whole").over(w_running).alias("last3_max_temp_c_whole"),
        F.count("temp_c_whole").over(w_running).alias("last3_count_temp_c_whole"),
        F.avg("temp_c_whole").over(w_running).alias("last3_avg_temp_c_whole"),
        F.row_number().over(w_grp).alias("seq_no"),
    )
)

gaps = (
    enriched
    .withColumn("session_start_flag", F.when(F.lag("reading_ts").over(w3).isNull() | (F.col("reading_ts") > F.lag("reading_ts").over(w3) + F.expr("INTERVAL 20 MINUTES")), 1).otherwise(0))
)

sessionized = (
    gaps
    .select(
        "id",
        "device_code",
        "reading_ts",
        "status_bits",
        "last3_max_temp_c_whole",
        "last3_count_temp_c_whole",
        "seq_no",
        F.sum("session_start_flag").over(w4).alias("session_no"),
    )
)

valid_device_readings = (
    sessionized
    .filter((F.col("reading_ts") >= F.lit("2025-09-01 07:00:00").cast("timestamp")) & F.col("device_code").isin(["DV-02", "DV-04", "DV-03"]))
    .select(
        "id",
        "device_code",
        "reading_ts",
        "status_bits",
        "last3_max_temp_c_whole",
        "last3_count_temp_c_whole",
        "seq_no",
        "session_no",
        F.initcap(F.trim("device_code")).alias("device_code_title"),
    )
)

ranked = (
    valid_device_readings
    .withColumn("row_num", F.row_number().over(w5))
    .filter(F.col("row_num") == 1)
)

cleaned = (
    ranked
    .filter((F.col("reading_ts") > F.lit("2025-09-01").cast("date")) & (F.col("status_bits") == 4))
    .select(
        "*",
        F.round((F.col("last3_max_temp_c_whole") * 0.9), 2).alias("adjusted_last3_max_temp_c_whole"),
    )
)

result = (
    cleaned
    .groupBy("session_no")
    .agg(
        F.count("*").alias("num_rows"),
        F.size(F.collect_set("device_code")).alias("num_device_codes"),
        F.count_if(F.col("device_code") == "DV-03").alias("dv_03_events"),
        F.count(F.when(F.col("device_code") == "DV-03", 1)).alias("dv_03_rows"),
        F.max(F.when(F.col("device_code") == "DV-02", F.col("adjusted_last3_max_temp_c_whole"))).alias("dv_02_max_adjusted_last3_max_temp_c_whole"),
        F.sum(F.when(F.col("device_code") == "DV-02", F.col("session_no")).otherwise(0)).alias("dv_02_session_no"),
    )
)

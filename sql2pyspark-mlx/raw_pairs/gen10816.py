from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("status_bits", "id")
w2 = Window.partitionBy(F.date_trunc("month", "reading_ts_date")).orderBy("reading_ts", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.orderBy(F.col("reading_ts").cast("bigint")).rangeBetween(-7200, Window.currentRow)
w4 = Window.orderBy(F.col("next_pressure_whole").asc_nulls_last())
w5 = Window.orderBy(F.col("next_pressure_whole").desc(), "id")
w6 = Window.orderBy("next_pressure_whole", "id")
w_ordered = Window.orderBy("pressure_whole", "id")
w_hist = Window.partitionBy(F.year("reading_ts")).orderBy(F.col("pressure_whole").desc_nulls_first())

base = (
    df_device_readings
    .filter((F.col("reading_ts") > F.lit("2025-09-01").cast("date")) & F.col("device_code").isin(["DV-04", "DV-03"]))
    .select(
        "id",
        "reading_ts",
        "temp_c",
        "humidity",
        "pressure",
        "status_bits",
        "firmware",
        F.initcap(F.trim("device_code")).alias("device_code_title"),
    )
)

filtered = (
    base
    .filter(F.col("temp_c").between(22, 27))
    .select(
        "*",
        F.hour("reading_ts").alias("reading_ts_hour"),
        F.trim("firmware").alias("clean_firmware"),
    )
)

valid = (
    filtered
    .filter(F.col("temp_c") >= 38)
    .select(
        "id",
        "reading_ts",
        "temp_c",
        "status_bits",
        "firmware",
        "device_code_title",
        F.ceil("pressure").alias("pressure_whole"),
    )
)

base_device_readings = (
    valid
    .filter((F.col("reading_ts") >= F.lit("2025-09-01").cast("date")) & F.col("status_bits").between(8, 9))
    .select("*", F.to_date("reading_ts").alias("reading_ts_date"))
)

enriched = (
    base_device_readings
    .filter(F.col("pressure_whole") > 41)
    .select(
        "id",
        "reading_ts",
        "firmware",
        "pressure_whole",
        "reading_ts_date",
        F.lead("pressure_whole").over(w).alias("next_pressure_whole"),
        F.lead("temp_c").over(w).alias("next_temp_c"),
        F.row_number().over(w).alias("event_seq"),
        F.sum("status_bits").over(w2).alias("running_status_bits"),
    )
)

enriched_device_readings = (
    enriched
    .withColumn("next_temp_c_count_2h", F.count("next_temp_c").over(w3))
    .withColumn("next_pressure_whole_dense_rank", F.dense_rank().over(w4))
    .withColumn("next_pressure_whole_bucket", F.ntile(5).over(w5))
    .withColumn("prev_next_temp_c", F.lag("next_temp_c", 2).over(w6))
)

metrics = (
    enriched_device_readings
    .select(
        "id",
        "pressure_whole",
        "reading_ts_date",
        "next_pressure_whole",
        "next_temp_c",
        "running_status_bits",
        "next_pressure_whole_dense_rank",
        F.lag("next_pressure_whole_dense_rank").over(w_ordered).alias("prev_next_pressure_whole_dense_rank"),
        F.lag("event_seq").over(w_ordered).alias("prev_event_seq"),
        F.row_number().over(w_ordered).alias("event_seq_2"),
        F.dense_rank().over(w_hist).alias("pressure_whole_dense_rank"),
    )
)

result = (
    metrics
    .select(
        "id",
        "pressure_whole",
        "reading_ts_date",
        "next_pressure_whole",
        "next_temp_c",
        "running_status_bits",
        "next_pressure_whole_dense_rank",
        "prev_next_pressure_whole_dense_rank",
        "prev_event_seq",
    )
    .orderBy("next_temp_c", "running_status_bits")
)

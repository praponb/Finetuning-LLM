from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code").orderBy("reading_ts", "id")
w2 = Window.partitionBy("device_code").orderBy("reading_ts", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy(F.month("reading_ts"), "device_code").orderBy("reading_ts", "id").rowsBetween(-3, Window.currentRow)
w4 = Window.partitionBy("device_code").orderBy("session_no", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("device_code").orderBy("temp_c", "id")
w6 = Window.partitionBy("device_code").orderBy(F.col("prev_session_no").asc_nulls_last(), "id")
w7 = Window.partitionBy("device_code").orderBy("next_is_new_burst", "id")
w8 = Window.partitionBy("device_code").orderBy("prev_session_no", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w9 = Window.partitionBy(F.to_date("reading_ts"), "device_code").orderBy(F.col("temp_c_int").desc(), "id")
w10 = Window.partitionBy("device_code").orderBy(F.col("reading_ts").cast("bigint")).rangeBetween(-1800, Window.currentRow)
w11 = Window.orderBy("session_no_avg_3", "id")

gaps = (
    df_device_readings
    .withColumn("is_new_burst", F.when(F.col("reading_ts") - F.lag("reading_ts").over(w) > F.expr("INTERVAL 20 MINUTES"), 1).otherwise(0))
)

sessions_device_readings = gaps.withColumn("session_no", F.sum("is_new_burst").over(w2))

filtered = (
    sessions_device_readings
    .filter((F.col("reading_ts") > F.lit("2025-09-01 02:00:00").cast("timestamp")) & (F.col("session_no") <= 8))
    .withColumn("temp_c_int", F.col("temp_c").cast("int"))
    .withColumn("device_code_group", F.when(F.col("device_code").isin(["DV-02", "DV-03"]), "Core").otherwise("Other"))
)

enriched = (
    filtered
    .select(
        "id",
        "device_code",
        "reading_ts",
        "temp_c",
        "firmware",
        "is_new_burst",
        "session_no",
        "temp_c_int",
        "device_code_group",
        F.avg("session_no").over(w3).alias("session_no_avg_3"),
        F.sum("status_bits").over(w4).alias("running_status_bits"),
    )
)

with_windows = (
    enriched
    .filter(F.col("reading_ts") >= "2023-06-01")
    .select(
        "*",
        F.lead("is_new_burst").over(w5).alias("next_is_new_burst"),
        F.lag("is_new_burst").over(w5).alias("prev_is_new_burst"),
        F.lag("session_no").over(w5).alias("prev_session_no"),
        F.lead("session_no").over(w5).alias("next_session_no"),
        F.row_number().over(w5).alias("row_seq"),
    )
)

latest = (
    with_windows
    .withColumn("row_num", F.row_number().over(w6))
    .filter(F.col("row_num") == 1)
)

result = (
    latest
    .select(
        "id",
        "device_code",
        "reading_ts",
        "temp_c",
        "temp_c_int",
        "session_no_avg_3",
        "running_status_bits",
        "row_num",
        F.lag("session_no_avg_3", 1).over(w7).alias("prev_session_no_avg_3"),
        F.last("temp_c_int", ignorenulls=True).over(w8).alias("last_known_temp_c_int"),
        F.row_number().over(w9).alias("row_seq_2"),
        F.avg("temp_c_int").over(w10).alias("temp_c_int_avg_30m"),
        F.lag("temp_c_int", 2).over(w11).alias("prev_temp_c_int"),
    )
)

from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code").orderBy("reading_ts", "id")
w2 = Window.partitionBy("device_code").orderBy("reading_ts", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

base = (
    df_device_readings
    .filter(F.col("firmware") != "2.0.1")
    .select(
        "id",
        "device_code",
        "reading_ts",
        "temp_c",
        "humidity",
        "pressure",
        "status_bits",
        "firmware",
        (F.col("status_bits") - F.col("humidity")).alias("status_bits_minus_humidity"),
        F.when(F.col("firmware") == "1.0.3", 1).otherwise(0).alias("is_1_0_3"),
    )
)

flagged = (
    base
    .withColumn("new_group_flag", F.when(F.col("reading_ts") - F.lag("reading_ts").over(w) > F.expr("INTERVAL 1 HOUR"), 1).otherwise(0))
)

sessionized = (
    flagged
    .select(
        "id",
        "device_code",
        "reading_ts",
        "status_bits_minus_humidity",
        "is_1_0_3",
        F.sum("new_group_flag").over(w2).alias("session_id"),
    )
)

result = (
    sessionized
    .groupBy(
        F.when(F.col("session_id") >= 6, "PREMIUM").otherwise("STANDARD").alias("session_id_tier"),
        F.when(F.col("status_bits_minus_humidity") >= 65, "HIGH").otherwise("LOW").alias("status_bits_minus_humidity_tier"),
    )
    .agg(
        F.count(F.when(F.col("device_code") == "DV-02", 1)).alias("dv_02_rows"),
        F.count(F.when(F.col("device_code") == "DV-04", 1)).alias("dv_04_count"),
        F.size(F.collect_set("device_code")).alias("num_device_codes"),
        F.sum(F.when(F.col("device_code") == "DV-03", F.col("session_id")).otherwise(0)).alias("dv_03_session_id"),
    )
    .orderBy(F.col("session_id_tier").desc())
)

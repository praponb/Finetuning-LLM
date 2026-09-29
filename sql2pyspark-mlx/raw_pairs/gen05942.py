from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code").orderBy("reading_ts", "id")
w2 = Window.partitionBy("device_code").orderBy("reading_ts", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.orderBy(F.col("session_no").desc(), "id")
w4 = Window.orderBy("reading_ts", "id").rowsBetween(-3, -1)

base = (
    df_device_readings
    .filter(F.col("reading_ts") > F.lit("2025-09-01").cast("date"))
    .select(
        "id",
        "device_code",
        "reading_ts",
        "temp_c",
        "humidity",
        "status_bits",
        "firmware",
        F.greatest("pressure", "temp_c").alias("greatest_pressure_temp_c"),
    )
)

gaps = (
    base
    .withColumn("is_new_session", F.when(F.lag("reading_ts").over(w).isNull() | (F.col("reading_ts") > F.lag("reading_ts").over(w) + F.expr("INTERVAL 30 MINUTES")), 1).otherwise(0))
)

islands = (
    gaps
    .select(
        "id",
        "device_code",
        "reading_ts",
        "humidity",
        "firmware",
        F.sum("is_new_session").over(w2).alias("session_no"),
    )
)

base_device_readings = (
    islands
    .filter(F.col("reading_ts") > F.to_timestamp(F.lit("2025-09-01 02:00:00")))
    .select(
        "id",
        "reading_ts",
        "humidity",
        "firmware",
        "session_no",
        F.when(F.col("device_code").isin(["DV-03", "DV-04"]), "Group A").otherwise("Other").alias("device_code_group"),
        (F.col("humidity") - F.col("session_no")).alias("humidity_minus_session_no"),
    )
)

metrics = (
    base_device_readings
    .filter(F.col("humidity_minus_session_no") < 50)
    .select(
        "*",
        F.ntile(5).over(w3).alias("session_no_bucket"),
        F.avg("humidity_minus_session_no").over(w4).alias("humidity_minus_session_no_avg_3_prior"),
    )
)

cleaned = (
    metrics
    .filter(F.col("reading_ts") > F.to_timestamp(F.lit("2025-09-01 05:00:00")))
    .select(
        "id",
        "reading_ts",
        "humidity",
        "firmware",
        "device_code_group",
        "humidity_minus_session_no",
        "humidity_minus_session_no_avg_3_prior",
        F.coalesce("session_no_bucket", F.lit(0)).alias("session_no_bucket_filled"),
        F.when(F.col("session_no") >= 9, "High").when(F.col("session_no") >= 3, "Medium").otherwise("Low").alias("session_no_level"),
    )
)

result = (
    cleaned
    .select(
        "id",
        "reading_ts",
        "humidity",
        "device_code_group",
        "humidity_minus_session_no_avg_3_prior",
        "session_no_level",
    )
)

from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("temp_c", "id")
w2 = Window.partitionBy("device_code").orderBy("reading_ts", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("device_code")
w4 = Window.partitionBy("device_code").orderBy(F.col("running_status_bits_times_temp_c").desc(), "id")

filtered = (
    df_device_readings
    .filter(F.col("reading_ts") > F.lit("2025-09-01").cast("date"))
    .select(
        "id",
        "device_code",
        "reading_ts",
        "temp_c",
        "humidity",
        "pressure",
        "status_bits",
        (F.col("status_bits") * F.col("temp_c")).alias("status_bits_times_temp_c"),
    )
)

enriched = (
    filtered
    .withColumn("next_pressure", F.lead("pressure", 1).over(w))
    .withColumn("running_status_bits_times_temp_c", F.sum("status_bits_times_temp_c").over(w2))
    .withColumn("humidity_share", F.round((F.col("humidity") / F.sum("humidity").over(w3)), 4))
)

deduped = (
    enriched
    .withColumn("row_num", F.row_number().over(w4))
    .filter(F.col("row_num") == 1)
)

stats = (
    deduped
    .groupBy(F.date_trunc("HOUR", "reading_ts").alias("hour_window"))
    .agg(
        F.max("running_status_bits_times_temp_c").alias("max_running_status_bits_times_temp_c"),
        F.percentile_approx("temp_c", [0.25, 0.5, 0.75]).alias("temp_c_quartiles"),
    )
    .filter(F.col("max_running_status_bits_times_temp_c") < 0)
)

result = (
    stats
    .select("hour_window", "max_running_status_bits_times_temp_c", "temp_c_quartiles")
    .orderBy(F.col("hour_window").desc())
)

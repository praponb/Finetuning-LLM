from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code").orderBy("reading_ts", "id")
w2 = Window.partitionBy("device_code").orderBy("reading_ts", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy(F.month("reading_ts"), "device_code").orderBy("session_no", "id").rowsBetween(-2, Window.currentRow)
w_hist = Window.partitionBy(F.year("reading_ts"), "device_code").orderBy("days_since_reading_ts", "id")
w5 = Window.orderBy("humidity", "id").rowsBetween(-5, -1)
w6 = Window.partitionBy("device_code").orderBy(F.col("new_group_flag").asc_nulls_last())

base = (
    df_device_readings
    .filter(F.col("reading_ts") >= F.lit("2025-09-01 01:00:00").cast("timestamp"))
    .select(
        "id",
        "device_code",
        "reading_ts",
        "temp_c",
        "humidity",
        "pressure",
        "status_bits",
        "firmware",
        F.datediff(F.current_date(), "reading_ts").alias("days_since_reading_ts"),
        (F.col("temp_c") + F.col("pressure")).alias("temp_c_plus_pressure"),
    )
)

gaps = (
    base
    .withColumn("new_group_flag", F.when(F.col("reading_ts") - F.lag("reading_ts").over(w) > F.expr("INTERVAL 45 MINUTES"), 1).otherwise(0))
)

sessionized = gaps.withColumn("session_no", F.sum("new_group_flag").over(w2))

metrics = (
    sessionized
    .withColumn("status_bits_avg_2", F.avg("status_bits").over(w3))
    .withColumn("next_pressure", F.lead("pressure", 1).over(w_hist))
    .withColumn("temp_c_avg_5_prior", F.avg("temp_c").over(w5))
)

deduped = (
    metrics
    .withColumn("rank_in_group", F.rank().over(w6))
    .filter(F.col("rank_in_group") == 1)
)

cleaned = (
    deduped
    .filter(F.col("humidity") <= 40)
    .select("*", F.round("rank_in_group", 1).alias("rank_in_group_rounded"))
)

result = (
    cleaned
    .select(
        "id",
        "reading_ts",
        "humidity",
        "firmware",
        "temp_c_plus_pressure",
        "new_group_flag",
        "session_no",
        "status_bits_avg_2",
        "next_pressure",
    )
    .orderBy(F.col("id").desc_nulls_last(), F.col("new_group_flag").desc())
)

from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code").orderBy("reading_ts", "id")
w2 = Window.partitionBy("device_code").orderBy("reading_ts", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("device_code").orderBy(F.col("pressure").asc_nulls_first(), "id")
w4 = Window.orderBy("reading_ts", "id")
w5 = Window.partitionBy(F.month("reading_ts"), "device_code").orderBy("reading_ts", "id").rowsBetween(-3, Window.currentRow)
w6 = Window.partitionBy("device_code").orderBy(F.col("next_session_no").desc_nulls_first(), "id")
w7 = Window.orderBy(F.col("next_session_no").desc_nulls_first())
w8 = Window.orderBy(F.col("next_session_no").desc())

gaps = (
    df_device_readings
    .withColumn("is_new_session", F.when(F.col("reading_ts") - F.lag("reading_ts").over(w) > F.expr("INTERVAL 45 MINUTES"), 1).otherwise(0))
)

islands = gaps.withColumn("session_no", F.sum("is_new_session").over(w2))

latest = (
    islands
    .withColumn("rn", F.row_number().over(w3))
    .filter(F.col("rn") == 1)
)

scored = (
    latest
    .select(
        "id",
        "device_code",
        "reading_ts",
        "humidity",
        "status_bits",
        "is_new_session",
        F.lead("session_no", 2, 0).over(w4).alias("next_session_no"),
        F.avg("humidity").over(w5).alias("humidity_avg_3"),
    )
)

ranked = (
    scored
    .withColumn("latest_rank", F.row_number().over(w6))
    .filter(F.col("latest_rank") == 1)
    .select("id", "reading_ts", "next_session_no")
)

enriched = (
    ranked
    .withColumn("next_session_no_rank", F.rank().over(w7))
    .withColumn("next_session_no_cume_dist", F.cume_dist().over(w8))
)

cleaned = (
    enriched
    .filter((F.col("reading_ts") > F.lit("2025-09-01").cast("date")) & (F.col("next_session_no_cume_dist") > 0.79))
    .withColumn("reading_ts_period", F.date_trunc("YEAR", "reading_ts"))
    .withColumn("reading_ts_hour", F.hour("reading_ts"))
)

result = (
    cleaned
    .groupBy("next_session_no")
    .agg(
        F.min("reading_ts").alias("first_reading_ts"),
        F.percentile_approx("reading_ts_hour", [0.25, 0.5, 0.75]).alias("reading_ts_hour_quartiles"),
        F.count("*").alias("row_count"),
        F.max("reading_ts_hour").alias("max_reading_ts_hour"),
        F.round(F.avg("next_session_no_rank"), 2).alias("avg_next_session_no_rank"),
        F.max("reading_ts").alias("last_reading_ts"),
    )
    .filter(F.col("avg_next_session_no_rank") >= 0)
    .orderBy(F.col("next_session_no").asc_nulls_first())
)

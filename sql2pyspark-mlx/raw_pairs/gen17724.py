from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code").orderBy("reading_ts", "id")
w2 = Window.partitionBy("device_code").orderBy("reading_ts", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_recent = Window.partitionBy("device_code").orderBy("session_start_flag", "id")
w4 = Window.orderBy("next_status_bits", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.orderBy("next_status_bits", "id")
w_running = Window.orderBy(F.col("pressure").desc(), "id")
w_hist = Window.orderBy("seq_no", "id").rowsBetween(-2, Window.currentRow)
w8 = Window.orderBy("temp_c", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

with_gaps = (
    df_device_readings
    .withColumn("session_start_flag", F.when(F.lag("reading_ts").over(w).isNull() | (F.col("reading_ts") > F.lag("reading_ts").over(w) + F.expr("INTERVAL 45 MINUTES")), 1).otherwise(0))
)

islands = with_gaps.withColumn("session_no", F.sum("session_start_flag").over(w2))

scored = (
    islands
    .filter(F.col("temp_c") > 25)
    .select(
        "id",
        "temp_c",
        "humidity",
        "pressure",
        "firmware",
        "session_no",
        F.lead("status_bits").over(w_recent).alias("next_status_bits"),
        F.lead("pressure").over(w_recent).alias("next_pressure"),
        F.row_number().over(w_recent).alias("seq_no"),
    )
)

base = (
    scored
    .filter(F.col("seq_no").between(4, 6))
    .withColumn("next_pressure_filled", F.coalesce("next_pressure", F.lit(0)))
)

enriched = (
    base
    .withColumn("cumulative_temp_c", F.sum("temp_c").over(w4))
    .withColumn("next_status_bits_bucket", F.ntile(10).over(w5))
    .withColumn("pressure_bucket", F.ntile(5).over(w_running))
    .withColumn("temp_c_avg_2", F.avg("temp_c").over(w_hist))
    .withColumn("running_seq_no", F.sum("seq_no").over(w8))
)

result = (
    enriched
    .groupBy("session_no")
    .agg(
        F.percentile_approx("next_status_bits_bucket", [0.25, 0.5, 0.75]).alias("next_status_bits_bucket_quartiles"),
        F.round(F.avg("session_no"), 2).alias("avg_session_no"),
        F.sum("humidity").alias("total_humidity"),
        F.percentile_approx("pressure", [0.25, 0.5, 0.75]).alias("pressure_quartiles"),
        F.percentile_approx("humidity", 0.5).alias("humidity_p50"),
    )
)

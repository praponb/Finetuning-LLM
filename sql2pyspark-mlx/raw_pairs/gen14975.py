from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code").orderBy(F.col("status_bits").desc(), "id")
w2 = Window.partitionBy("device_code").orderBy("reading_ts", "id")
w3 = Window.partitionBy("device_code").orderBy("reading_ts", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("device_code").orderBy(F.col("humidity").asc_nulls_last(), "id")
w5 = Window.partitionBy("device_code").orderBy("burst_id", "id").rowsBetween(-5, -1)
w_recent = Window.orderBy(F.col("pressure").desc(), "id")

cleaned = (
    df_device_readings
    .filter(F.col("reading_ts") > F.lit("2025-09-01").cast("date"))
    .select("*", F.trim("device_code").alias("clean_device_code"))
)

ranked = (
    cleaned
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
)

gaps = (
    ranked
    .withColumn("is_new_burst", F.when(F.col("reading_ts") - F.lag("reading_ts").over(w2) > F.expr("INTERVAL 15 MINUTES"), 1).otherwise(0))
)

sessions_device_readings = gaps.withColumn("burst_id", F.sum("is_new_burst").over(w3))

deduped = (
    sessions_device_readings
    .withColumn("dedup_rank", F.row_number().over(w4))
    .filter(F.col("dedup_rank") == 1)
    .select(
        "id",
        "device_code",
        "temp_c",
        "humidity",
        "pressure",
        "status_bits",
        "firmware",
        "rn",
        "burst_id",
    )
)

enriched = (
    deduped
    .select(
        "id",
        "device_code",
        "firmware",
        "rn",
        F.avg("pressure").over(w5).alias("pressure_avg_5_prior"),
        F.row_number().over(w_recent).alias("row_seq"),
    )
)

base = (
    enriched
    .filter(F.col("pressure_avg_5_prior").between(1007, 1025))
    .withColumn("least_rn_row_seq", F.least("rn", "row_seq"))
)

result = (
    base
    .groupBy(
        F.when(F.col("row_seq") >= 8, "HIGH").otherwise("LOW").alias("row_seq_tier"),
    )
    .agg(
        F.sum(F.when(F.col("device_code") == "DV-03", F.col("least_rn_row_seq"))).alias("dv_03_total_least_rn_row_seq"),
        F.count(F.when(F.col("device_code") == "DV-01", 1)).alias("dv_01_rows"),
    )
    .filter(F.col("dv_01_rows") > 1)
    .orderBy(F.col("dv_01_rows").desc_nulls_first())
)

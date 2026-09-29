from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code").orderBy(F.col("status_bits_per_pressure").desc_nulls_last(), "id")
w2 = Window.partitionBy("device_code").orderBy("reading_ts", "id")
w3 = Window.partitionBy("device_code").orderBy("reading_ts", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("device_code").orderBy(F.col("reading_ts").desc(), "id")
w5 = Window.partitionBy("device_code").orderBy(F.col("status_bits").desc())
w6 = Window.partitionBy("device_code").orderBy("island_id_2")
w7 = Window.orderBy("status_bits_per_pressure")
w8 = Window.partitionBy(F.to_date("reading_ts"), "device_code").orderBy("temp_c", "id")
w9 = Window.partitionBy("device_code").orderBy("status_bits_dense_rank", "id")
w10 = Window.partitionBy("device_code").orderBy("island_id", "id").rowsBetween(-2, -1)
w11 = Window.partitionBy("device_code").orderBy("island_id", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_recent = Window.partitionBy("device_code")

base = (
    df_device_readings
    .filter(F.col("reading_ts") > F.lit("2025-09-01").cast("date"))
    .select(
        "*",
        F.round((F.col("status_bits") / F.col("pressure")), 2).alias("status_bits_per_pressure"),
    )
)

ranked = (
    base
    .withColumn("dedup_rank", F.row_number().over(w))
    .filter(F.col("dedup_rank") == 1)
    .drop("dedup_rank")
)

prev_events = ranked.withColumn("prev_reading_ts", F.lag("reading_ts").over(w2))

with_gaps = (
    prev_events
    .withColumn("is_new_burst", F.when(F.col("prev_reading_ts").isNull() | (F.col("reading_ts") > F.col("prev_reading_ts") + F.expr("INTERVAL 20 MINUTES")), 1).otherwise(0))
)

sessionized = (
    with_gaps
    .select(
        "id",
        "device_code",
        "reading_ts",
        "temp_c",
        "humidity",
        "status_bits",
        "status_bits_per_pressure",
        F.sum("is_new_burst").over(w3).alias("island_id"),
    )
)

gaps = (
    sessionized
    .withColumn("session_start_flag", F.when(F.lag("reading_ts").over(w2).isNull() | (F.col("reading_ts") > F.lag("reading_ts").over(w2) + F.expr("INTERVAL 45 MINUTES")), 1).otherwise(0))
)

islands = gaps.withColumn("island_id_2", F.sum("session_start_flag").over(w3))

ranked_device_readings = (
    islands
    .withColumn("row_num", F.row_number().over(w4))
    .filter(F.col("row_num") == 1)
)

metrics = (
    ranked_device_readings
    .select(
        "id",
        "device_code",
        "temp_c",
        "humidity",
        "status_bits",
        "status_bits_per_pressure",
        "island_id",
        "island_id_2",
        "row_num",
        F.dense_rank().over(w5).alias("status_bits_dense_rank"),
        F.percent_rank().over(w6).alias("island_id_2_percent_rank"),
        F.percent_rank().over(w7).alias("status_bits_per_pressure_percent_rank"),
        F.lead("island_id", 2, 0).over(w8).alias("next_island_id"),
    )
)

result = (
    metrics
    .filter(F.col("status_bits_per_pressure") < 25)
    .select(
        "id",
        "status_bits",
        "status_bits_per_pressure",
        "island_id_2",
        "row_num",
        "status_bits_per_pressure_percent_rank",
        "next_island_id",
        F.row_number().over(w9).alias("row_seq"),
        F.avg("island_id_2").over(w10).alias("island_id_2_avg_2_prior"),
        F.sum("temp_c").over(w11).alias("running_temp_c"),
        F.min("status_bits_per_pressure").over(w_recent).alias("device_code_min_status_bits_per_pressure"),
    )
)

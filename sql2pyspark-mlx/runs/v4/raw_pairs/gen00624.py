from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code").orderBy("reading_ts", "id")
w2 = Window.partitionBy("device_code").orderBy("reading_ts", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("device_code").orderBy(F.col("new_group_flag").asc_nulls_first(), "id")
w4 = Window.partitionBy("device_code").orderBy("island_id", "id")
w5 = Window.partitionBy("device_code")
w6 = Window.orderBy("reading_ts", "id")
w7 = Window.orderBy(F.col("prev_reading_ts").cast("bigint")).rangeBetween(-7776000, Window.currentRow)

cleaned = (
    df_device_readings
    .filter(F.col("humidity") <= 54)
    .withColumn("reading_ts_label", F.date_format("reading_ts", "yyyy-MM"))
)

with_prev = cleaned.withColumn("prev_reading_ts", F.lag("reading_ts").over(w))

flagged = (
    with_prev
    .withColumn("new_group_flag", F.when(F.col("prev_reading_ts").isNull() | (F.col("reading_ts") > F.col("prev_reading_ts") + F.expr("INTERVAL 45 MINUTES")), 1).otherwise(0))
)

sessions_device_readings = flagged.withColumn("island_id", F.sum("new_group_flag").over(w2))

ranked = (
    sessions_device_readings
    .withColumn("rn", F.row_number().over(w3))
    .filter(F.col("rn") == 1)
)

with_windows = (
    ranked
    .withColumn("row_seq", F.row_number().over(w4))
    .withColumn("device_code_avg_humidity", F.avg("humidity").over(w5))
    .withColumn("row_seq_2", F.row_number().over(w6))
    .withColumn("humidity_share", F.round((F.col("humidity") / F.sum("humidity").over(w5)), 4))
    .withColumn("island_id_avg_90d", F.avg("island_id").over(w7))
)

result = (
    with_windows
    .withColumn("row_seq_2_tier", F.when(F.col("row_seq_2") >= 7, "PREMIUM").otherwise("STANDARD"))
    .rollup("device_code", "island_id", "row_seq_2_tier")
    .agg(
        F.percentile_approx("row_seq_2", [0.25, 0.5, 0.75]).alias("row_seq_2_quartiles"),
        F.sum(F.when(F.col("device_code") == "DV-01", F.col("new_group_flag")).otherwise(0)).alias("dv_01_new_group_flag"),
        F.round(F.avg("row_seq"), 2).alias("avg_row_seq"),
        F.sort_array(F.collect_set("device_code")).alias("device_code_list"),
        F.sum(F.when(F.col("device_code") == "DV-03", F.col("humidity_share")).otherwise(0)).alias("dv_03_humidity_share"),
        F.grouping_id().alias("grouping_level"),
    )
    .filter(F.col("dv_03_humidity_share") > 0)
)

from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code").orderBy("reading_ts", "id")
w2 = Window.partitionBy("device_code").orderBy("reading_ts", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

with_gaps = (
    df_device_readings
    .withColumn("new_group_flag", F.when(F.lag("reading_ts").over(w).isNull() | (F.col("reading_ts") > F.lag("reading_ts").over(w) + F.expr("INTERVAL 30 MINUTES")), 1).otherwise(0))
)

islands = with_gaps.withColumn("island_id", F.sum("new_group_flag").over(w2))

result = (
    islands
    .withColumn("activity_day", F.date_trunc("DAY", "reading_ts"))
    .groupingSets(
        [["island_id", "activity_day"], ["island_id"]],
        "island_id",
        "activity_day",
    )
    .agg(
        F.round(F.avg("island_id"), 2).alias("avg_island_id"),
        F.round(F.sum("humidity") / F.nullif(F.count(F.when(F.col("device_code") == "DV-01", 1)), F.lit(0)), 2).alias("humidity_per_dv_01"),
        F.size(F.collect_set("device_code")).alias("num_device_codes"),
        F.percentile_approx("island_id", 0.95).alias("island_id_p95"),
        F.bit_and("status_bits").alias("status_bits_bit_and"),
        F.count_if(F.col("device_code") == "DV-02").alias("dv_02_events"),
        F.grouping("island_id").alias("gid"),
    )
    .filter(F.col("dv_02_events") >= 1)
)

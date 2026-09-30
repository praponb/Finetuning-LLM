from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code").orderBy("reading_ts", "id")
w2 = Window.partitionBy("device_code").orderBy("reading_ts", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

with_prev = df_device_readings.withColumn("prev_reading_ts", F.lag("reading_ts").over(w))

flagged = (
    with_prev
    .withColumn("is_new_burst", F.when(F.col("prev_reading_ts").isNull() | (F.col("reading_ts") > F.col("prev_reading_ts") + F.expr("INTERVAL 15 MINUTES")), 1).otherwise(0))
)

sessions_device_readings = flagged.withColumn("burst_id", F.sum("is_new_burst").over(w2))

result = (
    sessions_device_readings
    .cube("burst_id")
    .agg(
        F.countDistinct("device_code").alias("distinct_device_codes"),
        F.bit_and("status_bits").alias("status_bits_bit_and"),
        F.sort_array(F.collect_set("device_code")).alias("device_code_list"),
        F.stddev_samp("pressure").alias("pressure_stddev"),
        F.max("prev_reading_ts").alias("last_prev_reading_ts"),
        F.bit_or("status_bits").alias("status_bits_bit_or"),
        F.grouping_id().alias("grouping_level"),
    )
)

from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code").orderBy("reading_ts", "id")
w2 = Window.partitionBy("device_code").orderBy("reading_ts", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

flagged = (
    df_device_readings
    .withColumn("is_new_burst", F.when(F.col("reading_ts") - F.lag("reading_ts").over(w) > F.expr("INTERVAL 30 MINUTES"), 1).otherwise(0))
)

sessions_device_readings = flagged.withColumn("session_no", F.sum("is_new_burst").over(w2))

result = (
    sessions_device_readings
    .groupBy(
        "device_code",
        F.when(F.col("pressure") >= 1032, "HIGH").otherwise("LOW").alias("pressure_tier"),
    )
    .agg(
        F.bit_and("status_bits").alias("status_bits_bit_and"),
        F.size(F.collect_set("device_code")).alias("num_device_codes"),
        ((F.unix_timestamp(F.max("reading_ts")) - F.unix_timestamp(F.min("reading_ts"))) / 60).alias("duration_min"),
        F.sort_array(F.collect_set("device_code")).alias("device_code_list"),
    )
    .orderBy(F.col("duration_min").asc_nulls_last())
)

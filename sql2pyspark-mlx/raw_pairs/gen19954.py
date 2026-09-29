from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code").orderBy("reading_ts", "id")
w2 = Window.partitionBy("device_code").orderBy("reading_ts", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

with_gaps = (
    df_device_readings
    .withColumn("is_new_burst", F.when(F.lag("reading_ts").over(w).isNull() | (F.col("reading_ts") > F.lag("reading_ts").over(w) + F.expr("INTERVAL 30 MINUTES")), 1).otherwise(0))
)

sessionized = with_gaps.withColumn("burst_id", F.sum("is_new_burst").over(w2))

result = (
    sessionized
    .groupBy("device_code")
    .agg(
        F.max("reading_ts").alias("last_reading_ts"),
        F.max("is_new_burst").alias("max_is_new_burst"),
        F.percentile_approx("temp_c", 0.5).alias("temp_c_p50"),
    )
    .orderBy(F.col("max_is_new_burst").desc_nulls_first())
)

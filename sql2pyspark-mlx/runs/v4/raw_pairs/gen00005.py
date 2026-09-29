from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code").orderBy("reading_ts", "id")
w2 = Window.partitionBy("device_code").orderBy("reading_ts", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

gaps = (
    df_device_readings
    .withColumn("is_new_burst", F.when(F.col("reading_ts") - F.lag("reading_ts").over(w) > F.expr("INTERVAL 15 MINUTES"), 1).otherwise(0))
)

islands = gaps.withColumn("island_id", F.sum("is_new_burst").over(w2))

result = (
    islands
    .groupBy(
        F.when(F.col("temp_c") >= 35.3, "PREMIUM").otherwise("STANDARD").alias("temp_c_tier"),
    )
    .agg(
        F.max("temp_c").alias("max_temp_c"),
        F.percentile_approx("pressure", 0.95).alias("pressure_p95"),
        F.size(F.collect_set("device_code")).alias("num_device_codes"),
        F.count("*").alias("cnt"),
        F.percentile_approx("temp_c", 0.9).alias("temp_c_p90"),
        F.sum(F.when(F.col("device_code") == "DV-03", F.col("island_id"))).alias("dv_03_total_island_id"),
    )
    .filter(F.col("dv_03_total_island_id") > 0)
    .orderBy(F.col("temp_c_p90").desc())
)

from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code").orderBy("reading_ts", "id")
w2 = Window.partitionBy("device_code").orderBy("reading_ts", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

filtered = (
    df_device_readings
    .filter(~F.col("device_code").isin(["DV-02", "DV-03", "DV-04"]))
    .select("*", F.round((F.col("temp_c") * 0.85), 2).alias("temp_c_adjusted"))
)

gaps = (
    filtered
    .withColumn("is_new_burst", F.when(F.lag("reading_ts").over(w).isNull() | (F.col("reading_ts") > F.lag("reading_ts").over(w) + F.expr("INTERVAL 1 HOUR")), 1).otherwise(0))
)

sessionized = gaps.withColumn("session_id", F.sum("is_new_burst").over(w2))

result = (
    sessionized
    .rollup(
        "device_code",
        F.when(F.col("session_id") >= 7, "PREMIUM").otherwise("STANDARD").alias("session_id_tier"),
        F.when(F.col("status_bits") >= 10, "HIGH").otherwise("LOW").alias("status_bits_tier"),
    )
    .agg(
        F.size(F.collect_set("device_code")).alias("num_device_codes"),
        F.count(F.when(F.col("device_code") == "DV-02", 1)).alias("dv_02_count"),
        F.count("*").alias("event_count"),
        F.sort_array(F.collect_set("device_code")).alias("device_code_list"),
    )
    .filter(F.col("event_count") > 1)
)

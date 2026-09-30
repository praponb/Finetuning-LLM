from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code").orderBy("temp_c", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy(F.to_date("reading_ts"), "device_code").orderBy(F.col("reading_ts").cast("bigint")).rangeBetween(-7200, Window.currentRow)
w3 = Window.orderBy("pressure", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

enriched = (
    df_device_readings
    .filter(F.col("device_code") != "DV-02")
    .withColumn("first_known_temp_c", F.first("temp_c", ignorenulls=True).over(w))
    .withColumn("status_bits_avg_2h", F.avg("status_bits").over(w2))
    .withColumn("first_known_humidity", F.first("humidity", ignorenulls=True).over(w3))
)

result = (
    enriched
    .groupBy("device_code")
    .agg(
        F.sort_array(F.collect_set("device_code")).alias("device_code_list"),
        F.percentile_approx("status_bits", 0.9).alias("status_bits_p90"),
        F.percentile_approx("pressure", [0.25, 0.5, 0.75]).alias("pressure_quartiles"),
    )
    .orderBy("status_bits_p90")
)

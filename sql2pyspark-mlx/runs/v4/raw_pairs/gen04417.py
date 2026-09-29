from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code").orderBy(F.col("pressure").desc(), "id")

latest = (
    df_device_readings
    .withColumn("rank_in_group", F.row_number().over(w))
    .filter(F.col("rank_in_group") == 1)
)

result = (
    latest
    .groupBy("device_code")
    .agg(
        F.percentile_approx("status_bits", [0.25, 0.5, 0.75]).alias("status_bits_quartiles"),
        F.count(F.when(F.col("device_code") == "DV-03", 1)).alias("dv_03_count"),
        F.min("reading_ts").alias("first_seen"),
        F.bit_and("status_bits").alias("status_bits_bit_and"),
        F.size(F.collect_set("device_code")).alias("num_device_codes"),
        F.percentile_approx("humidity", [0.25, 0.5, 0.75]).alias("humidity_quartiles"),
    )
    .filter(F.col("dv_03_count") < 1)
    .orderBy(F.col("dv_03_count").desc())
)

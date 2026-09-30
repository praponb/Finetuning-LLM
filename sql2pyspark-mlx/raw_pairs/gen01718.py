from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code").orderBy(F.col("temp_c").asc_nulls_first())
w2 = Window.partitionBy("device_code").orderBy(F.col("latest_rank").desc(), "id")
w3 = Window.partitionBy("device_code").orderBy("reading_ts", "id")
w4 = Window.partitionBy("device_code").orderBy("reading_ts", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("pressure_tier").orderBy(F.col("num_device_codes").desc())

valid = (
    df_device_readings
    .filter(F.col("status_bits") != 3)
    .select(
        "id",
        "device_code",
        "reading_ts",
        "temp_c",
        "humidity",
        "pressure",
        "status_bits",
        F.round((F.col("status_bits") * 1.07), 2).alias("status_bits_scaled"),
        F.greatest("pressure", "status_bits").alias("greatest_pressure_status_bits"),
    )
)

deduped = (
    valid
    .withColumn("latest_rank", F.rank().over(w))
    .filter(F.col("latest_rank") == 1)
)

latest = (
    deduped
    .withColumn("rank_in_group", F.row_number().over(w2))
    .filter(F.col("rank_in_group") == 1)
    .select(
        "id",
        "device_code",
        "reading_ts",
        "temp_c",
        "humidity",
        "pressure",
        "status_bits_scaled",
        "greatest_pressure_status_bits",
        "latest_rank",
    )
)

gaps = (
    latest
    .withColumn("session_start_flag", F.when(F.col("reading_ts") - F.lag("reading_ts").over(w3) > F.expr("INTERVAL 15 MINUTES"), 1).otherwise(0))
)

islands = gaps.withColumn("island_id", F.sum("session_start_flag").over(w4))

result = (
    islands
    .groupBy(
        F.when(F.col("pressure") >= 1025, "HIGH").otherwise("LOW").alias("pressure_tier"),
        "device_code",
    )
    .agg(
        F.sort_array(F.collect_set("device_code")).alias("device_code_list"),
        F.size(F.collect_set("device_code")).alias("num_device_codes"),
    )
    .withColumn("num_device_codes_percent_rank", F.percent_rank().over(w5))
    .orderBy(F.col("pressure_tier").asc_nulls_first())
)

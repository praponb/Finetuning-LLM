from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code").orderBy("status_bits", "id")
w2 = Window.partitionBy("device_code")
w3 = Window.partitionBy("device_code").orderBy("reading_ts", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("device_code").orderBy("temp_c", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("device_code").orderBy(F.col("temp_c").desc(), "id")
w6 = Window.orderBy(F.col("device_code_min_humidity").desc(), "id")
w_grp = Window.partitionBy("device_code").orderBy("humidity", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w8 = Window.partitionBy("device_code").orderBy("prev_humidity", "id")
w9 = Window.partitionBy("device_code").orderBy("corr_humidity_temp_c", "id")
w10 = Window.partitionBy("device_code").orderBy(F.col("humidity").desc_nulls_first())
w11 = Window.partitionBy("device_code").orderBy(F.col("seq_no").desc(), "id")
w12 = Window.partitionBy("device_code").orderBy(F.col("reading_ts").desc())
w13 = Window.partitionBy("device_code").orderBy(F.col("dedup_rank").desc(), "id")

enriched = (
    df_device_readings
    .withColumn("prev_humidity", F.lag("humidity", 1, 0).over(w))
    .withColumn("device_code_min_humidity", F.min("humidity").over(w2))
    .withColumn("running_humidity", F.sum("humidity").over(w3))
    .withColumn("last_known_pressure", F.last("pressure", ignorenulls=True).over(w4))
    .withColumn("temp_c_bucket", F.ntile(10).over(w5))
)

enriched_device_readings = (
    enriched
    .withColumn("row_seq", F.row_number().over(w6))
    .withColumn("cumulative_temp_c_bucket", F.sum("temp_c_bucket").over(w_grp))
    .withColumn("corr_humidity_temp_c", F.corr("humidity", "temp_c").over(w2))
)

with_windows = (
    enriched_device_readings
    .select(
        "*",
        F.row_number().over(w8).alias("seq_no"),
        F.lead("running_humidity", 1).over(w9).alias("next_running_humidity"),
        F.dense_rank().over(w10).alias("humidity_dense_rank"),
    )
)

deduped = (
    with_windows
    .withColumn("latest_rank", F.row_number().over(w11))
    .filter(F.col("latest_rank") == 1)
    .drop("latest_rank")
)

ranked = (
    deduped
    .withColumn("dedup_rank", F.rank().over(w12))
    .filter(F.col("dedup_rank") == 1)
)

valid = (
    ranked
    .filter(F.col("firmware").isin(["1.0.3", "1.1.0", "2.0.1"]))
    .withColumn("cumulative_temp_c_bucket_num", F.col("cumulative_temp_c_bucket").cast("double"))
)

ranked_device_readings = (
    valid
    .withColumn("row_num", F.row_number().over(w13))
    .filter(F.col("row_num") == 1)
)

result = (
    ranked_device_readings
    .select(
        "id",
        "device_code",
        F.col("humidity").alias("HUMIDITY"),
        F.col("last_known_pressure").alias("LAST_KNOWN_PRESSURE_VAL"),
        "pressure",
        F.col("corr_humidity_temp_c").alias("CORR_HUMIDITY_TEMP_C"),
    )
    .unpivot(
        ["id", "device_code"],
        ["HUMIDITY", "LAST_KNOWN_PRESSURE_VAL", "pressure", "CORR_HUMIDITY_TEMP_C"],
        "metric",
        "reading",
    )
    .filter(F.col("reading").isNotNull())
)

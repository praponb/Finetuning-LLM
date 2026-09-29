from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code").orderBy("reading_ts", "id")
w2 = Window.partitionBy("device_code").orderBy("reading_ts", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("device_code").orderBy("temp_c", "id")
w4 = Window.orderBy("reading_ts", "id")
w5 = Window.orderBy(F.col("next_temp_c").desc_nulls_first())
w6 = Window.orderBy("row_seq", "id")
w7 = Window.orderBy("next_humidity", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_part = Window.orderBy("prev_burst_id", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

base = (
    df_device_readings
    .filter(F.col("status_bits") != 4)
    .withColumn("firmware_device_code_key", F.concat_ws("-", "firmware", "device_code"))
)

flagged = (
    base
    .withColumn("is_new_burst", F.when(F.col("reading_ts") - F.lag("reading_ts").over(w) > F.expr("INTERVAL 45 MINUTES"), 1).otherwise(0))
)

islands = flagged.withColumn("burst_id", F.sum("is_new_burst").over(w2))

metrics = (
    islands
    .select(
        "id",
        "temp_c",
        "firmware",
        F.lead("humidity").over(w3).alias("next_humidity"),
        F.lag("burst_id").over(w3).alias("prev_burst_id"),
        F.row_number().over(w3).alias("row_seq"),
        F.lead("temp_c", 2).over(w4).alias("next_temp_c"),
    )
)

valid = (
    metrics
    .filter(F.col("firmware") == "1.0.3")
    .select(
        "*",
        F.floor("temp_c").alias("temp_c_whole"),
        F.col("next_humidity").cast("int").alias("next_humidity_int"),
    )
)

result = (
    valid
    .filter(F.col("temp_c_whole") > 72)
    .select(
        "*",
        F.rank().over(w5).alias("next_temp_c_rank"),
        F.row_number().over(w6).alias("row_seq_2"),
        F.first("temp_c", ignorenulls=True).over(w7).alias("first_known_temp_c"),
        F.sum("prev_burst_id").over(w_part).alias("cumulative_prev_burst_id"),
    )
)

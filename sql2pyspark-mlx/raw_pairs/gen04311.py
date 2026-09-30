from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_recent = Window.partitionBy("device_code").orderBy("status_bits", "id")
w2 = Window.orderBy("temp_c", "id").rowsBetween(-2, Window.currentRow)
w3 = Window.partitionBy("device_code").orderBy(F.col("status_bits_avg_2").desc_nulls_last(), "id")
w4 = Window.partitionBy("device_code").orderBy(F.col("prev_temp_c_minus_humidity").desc(), "id")
w_hist = Window.partitionBy("device_code").orderBy(F.col("event_seq").asc_nulls_first())

scored = (
    df_device_readings
    .select(
        "id",
        "device_code",
        "humidity",
        "firmware",
        F.lead("temp_c").over(w_recent).alias("next_temp_c"),
        F.lag("temp_c").over(w_recent).alias("prev_temp_c"),
        F.lag("pressure").over(w_recent).alias("prev_pressure"),
        F.lead("pressure").over(w_recent).alias("next_pressure"),
        F.row_number().over(w_recent).alias("event_seq"),
        F.avg("status_bits").over(w2).alias("status_bits_avg_2"),
    )
)

deduped = (
    scored
    .withColumn("row_num", F.row_number().over(w3))
    .filter(F.col("row_num") == 1)
)

valid = (
    deduped
    .filter(F.col("firmware") == "1.1.0")
    .select(
        "id",
        "device_code",
        "humidity",
        "firmware",
        "next_temp_c",
        "prev_pressure",
        "next_pressure",
        "event_seq",
        "row_num",
        (F.col("prev_temp_c") - F.col("humidity")).alias("prev_temp_c_minus_humidity"),
        F.col("next_pressure").cast("int").alias("next_pressure_int"),
    )
)

with_windows = (
    valid
    .withColumn("prev_temp_c_minus_humidity_bucket", F.ntile(4).over(w4))
    .withColumn("event_seq_dense_rank", F.dense_rank().over(w_hist))
)

result = (
    with_windows
    .select("id", "device_code", "prev_pressure", "next_pressure")
    .unpivot(
        ids=["id", "device_code"],
        values=["prev_pressure", "next_pressure"],
        variableColumnName="metric_name",
        valueColumnName="val",
    )
    .filter(F.col("val").isNotNull())
)

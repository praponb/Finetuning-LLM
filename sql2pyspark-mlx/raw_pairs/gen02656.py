from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.month("reading_ts_day")).orderBy(F.col("temp_c").desc(), "id")
w2 = Window.partitionBy("device_code").orderBy("status_bits_adjusted", "id").rowsBetween(-3, -1)
w3 = Window.partitionBy(F.year("reading_ts_day")).orderBy(F.col("status_bits_adjusted").desc())
w4 = Window.partitionBy("device_code").orderBy("status_bits_adjusted", "id")
w5 = Window.partitionBy(F.date_trunc("YEAR", "reading_ts_day")).orderBy(F.col("temp_c_avg_3_prior").desc(), "id")
w6 = Window.orderBy(F.col("temp_c").asc_nulls_last())
w7 = Window.orderBy("status_bits_adjusted", "id")

valid = (
    df_device_readings
    .filter(F.col("humidity") < 51)
    .select(
        "id",
        "device_code",
        "temp_c",
        "humidity",
        "pressure",
        "firmware",
        F.round((F.col("status_bits") * 1.1), 2).alias("status_bits_adjusted"),
        F.col("reading_ts").cast("date").alias("reading_ts_day"),
    )
)

scored = (
    valid
    .withColumn("row_seq", F.row_number().over(w))
    .withColumn("temp_c_avg_3_prior", F.avg("temp_c").over(w2))
    .withColumn("status_bits_adjusted_percent_rank", F.percent_rank().over(w3))
    .withColumn("prev_status_bits_adjusted", F.lag("status_bits_adjusted", 1).over(w4))
)

enriched = (
    scored
    .select(
        "id",
        "temp_c",
        "humidity",
        "status_bits_adjusted",
        "status_bits_adjusted_percent_rank",
        F.row_number().over(w5).alias("row_seq_2"),
        F.cume_dist().over(w6).alias("temp_c_cume_dist"),
    )
)

enriched_device_readings = (
    enriched
    .select(
        "*",
        F.lag("row_seq_2").over(w7).alias("prev_row_seq_2"),
        F.lead("row_seq_2").over(w7).alias("next_row_seq_2"),
        F.lead("status_bits_adjusted_percent_rank").over(w7).alias("next_status_bits_adjusted_percent_rank"),
        F.row_number().over(w7).alias("event_seq"),
    )
)

result = (
    enriched_device_readings
    .select(
        "id",
        "temp_c",
        "humidity",
        "temp_c_cume_dist",
        "prev_row_seq_2",
        "event_seq",
    )
)

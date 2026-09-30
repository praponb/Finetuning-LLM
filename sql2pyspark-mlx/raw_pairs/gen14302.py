from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code").orderBy("temp_c_minus_pressure", "id")
w_ordered = Window.partitionBy("device_code").orderBy("pressure", "id")
w_hist = Window.partitionBy("device_code").orderBy(F.col("humidity").desc_nulls_last())
w4 = Window.partitionBy("device_code").orderBy(F.col("humidity").asc_nulls_first(), "id")
w_grp = Window.orderBy("next_status_bits_whole", "id").rowsBetween(-2, Window.currentRow)
w6 = Window.orderBy("temp_c", "id").rowsBetween(-6, -1)

filtered = (
    df_device_readings
    .filter(F.col("pressure").between(1009, 1023))
    .select(
        "id",
        "device_code",
        "temp_c",
        "humidity",
        "pressure",
        "status_bits",
        "firmware",
        (F.col("temp_c") - F.col("pressure")).alias("temp_c_minus_pressure"),
    )
)

filtered_device_readings = (
    filtered
    .filter(F.col("status_bits").between(8, 10))
    .select("*", F.ceil("status_bits").alias("status_bits_whole"))
)

deduped = (
    filtered_device_readings
    .withColumn("rank_in_group", F.row_number().over(w))
    .filter(F.col("rank_in_group") == 1)
    .drop("rank_in_group")
)

scored = (
    deduped
    .filter(F.col("device_code") == "DV-01")
    .select(
        "id",
        "device_code",
        "temp_c",
        "humidity",
        "firmware",
        "temp_c_minus_pressure",
        F.lag("status_bits_whole").over(w_ordered).alias("prev_status_bits_whole"),
        F.lead("status_bits_whole").over(w_ordered).alias("next_status_bits_whole"),
        F.lag("temp_c_minus_pressure").over(w_ordered).alias("prev_temp_c_minus_pressure"),
        F.lead("temp_c_minus_pressure").over(w_ordered).alias("next_temp_c_minus_pressure"),
        F.row_number().over(w_ordered).alias("seq_no"),
        F.dense_rank().over(w_hist).alias("humidity_dense_rank"),
    )
)

base = (
    scored
    .filter(F.col("next_temp_c_minus_pressure").between(51, 58))
    .withColumn("device_code_title", F.initcap(F.trim("device_code")))
)

latest = (
    base
    .withColumn("latest_rank", F.row_number().over(w4))
    .filter(F.col("latest_rank") == 1)
    .select(
        "id",
        "temp_c",
        "humidity",
        "temp_c_minus_pressure",
        "prev_status_bits_whole",
        "next_status_bits_whole",
        "prev_temp_c_minus_pressure",
        "next_temp_c_minus_pressure",
        "seq_no",
        "humidity_dense_rank",
        "device_code_title",
    )
)

scored_device_readings = (
    latest
    .withColumn("last3_min_seq_no", F.min("seq_no").over(w_grp))
    .withColumn("last3_max_seq_no", F.max("seq_no").over(w_grp))
    .withColumn("last3_sum_seq_no", F.sum("seq_no").over(w_grp))
    .withColumn("prev_status_bits_whole_avg_6_prior", F.avg("prev_status_bits_whole").over(w6))
)

result = (
    scored_device_readings
    .select(
        "id",
        "humidity",
        "temp_c_minus_pressure",
        "prev_status_bits_whole",
        "next_status_bits_whole",
        "prev_temp_c_minus_pressure",
        "next_temp_c_minus_pressure",
        "humidity_dense_rank",
        "device_code_title",
    )
)

from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_grp = Window.partitionBy("device_code").orderBy("least_pressure_humidity", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.orderBy(F.col("temp_c").desc(), "id")
w3 = Window.partitionBy("device_code").orderBy("pressure", "id").rowsBetween(-3, Window.currentRow)
w_hist = Window.orderBy("least_status_bits_humidity", "id")
w5 = Window.orderBy(F.col("pressure").desc(), "id")
w6 = Window.partitionBy("device_code").orderBy(F.col("pressure_bucket").desc_nulls_first(), "id")

cleaned = (
    df_device_readings
    .filter(F.col("status_bits") < 10)
    .withColumn("least_pressure_humidity", F.least("pressure", "humidity"))
    .withColumn("least_status_bits_humidity", F.least("status_bits", "humidity"))
)

with_windows = (
    cleaned
    .filter(F.col("firmware") == "2.0.1")
    .select(
        "id",
        "device_code",
        "temp_c",
        "humidity",
        "pressure",
        "least_status_bits_humidity",
        F.first("pressure", ignorenulls=True).over(w_grp).alias("first_known_pressure"),
        F.row_number().over(w2).alias("row_seq"),
    )
)

scored = (
    with_windows
    .withColumn("least_status_bits_humidity_avg_3", F.avg("least_status_bits_humidity").over(w3))
    .withColumn("least_status_bits_humidity_bucket", F.ntile(3).over(w_hist))
    .withColumn("pressure_bucket", F.ntile(5).over(w5))
)

cleaned_device_readings = (
    scored
    .filter(F.col("least_status_bits_humidity_bucket") > 4)
    .select(
        "*",
        F.when(F.col("humidity") >= 82, "High").when(F.col("humidity") >= 41, "Medium").otherwise("Low").alias("humidity_band"),
    )
)

ranked = (
    cleaned_device_readings
    .withColumn("rn", F.row_number().over(w6))
    .filter(F.col("rn") == 1)
)

result = (
    ranked
    .groupBy(
        F.when(F.col("least_status_bits_humidity_bucket") >= 9, "PREMIUM").otherwise("STANDARD").alias("least_status_bits_humidity_bucket_tier"),
        "device_code",
    )
    .agg(
        F.sort_array(F.collect_set("device_code")).alias("device_code_list"),
        F.round(F.avg("least_status_bits_humidity_avg_3"), 2).alias("avg_least_status_bits_humidity_avg_3"),
        F.count_if(F.col("device_code") == "DV-02").alias("dv_02_events"),
    )
    .orderBy(F.col("device_code").desc())
)

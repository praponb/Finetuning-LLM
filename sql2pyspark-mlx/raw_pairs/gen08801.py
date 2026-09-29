from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code").orderBy(F.col("temp_c_int").desc(), "id")

valid = (
    df_device_readings
    .filter(F.col("reading_ts") > F.to_date(F.lit("2025-09-01")))
    .select(
        "id",
        "device_code",
        "reading_ts",
        "temp_c",
        "humidity",
        "pressure",
        "status_bits",
        "firmware",
        F.coalesce("pressure", F.lit(0)).alias("pressure_filled"),
        F.col("temp_c").cast("int").alias("temp_c_int"),
    )
)

result = (
    valid
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") == 1)
    .select("id", "device_code", "reading_ts", "temp_c", "pressure_filled")
)

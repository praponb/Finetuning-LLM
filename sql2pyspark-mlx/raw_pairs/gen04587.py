from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code").orderBy("temp_c", "id")
w_recent = Window.partitionBy("device_code").orderBy(F.col("pressure").asc_nulls_last())

result = (
    df_device_readings
    .select(
        "id",
        "device_code",
        "reading_ts",
        "humidity",
        "pressure",
        "status_bits",
        F.row_number().over(w).alias("row_seq"),
        F.cume_dist().over(w_recent).alias("pressure_cume_dist"),
    )
)

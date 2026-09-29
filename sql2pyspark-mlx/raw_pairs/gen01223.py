from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_ordered = Window.partitionBy(F.year("reading_ts"), "device_code").orderBy(F.col("status_bits").desc(), "id")
w_ordered_2 = Window.partitionBy("device_code").orderBy("status_bits")
w_recent = Window.partitionBy("device_code")

result = (
    df_device_readings
    .select(
        "id",
        "device_code",
        "reading_ts",
        "temp_c",
        "status_bits",
        F.ntile(5).over(w_ordered).alias("status_bits_bucket"),
        F.percent_rank().over(w_ordered_2).alias("status_bits_percent_rank"),
        F.count("pressure").over(w_recent).alias("device_code_count_pressure"),
    )
)

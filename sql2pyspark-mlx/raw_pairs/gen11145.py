from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code").orderBy("humidity", "id")

enriched = (
    df_device_readings
    .filter(F.col("status_bits") != 10)
    .select(
        "*",
        F.lag("status_bits").over(w).alias("prev_status_bits"),
        F.lag("humidity").over(w).alias("prev_humidity"),
        F.row_number().over(w).alias("seq_no"),
    )
)

result = (
    enriched
    .select(
        "id",
        "temp_c",
        "humidity",
        "pressure",
        "status_bits",
        "firmware",
        "prev_humidity",
    )
    .orderBy(F.col("prev_humidity").desc_nulls_last())
)

from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code").orderBy("pressure", "id")
w2 = Window.orderBy("pressure", "id")

result = (
    df_device_readings
    .select(
        "id",
        "reading_ts",
        "temp_c",
        "humidity",
        "status_bits",
        "firmware",
        F.lead("status_bits").over(w).alias("next_status_bits"),
        F.lag("pressure").over(w).alias("prev_pressure"),
        F.lead("pressure").over(w).alias("next_pressure"),
        F.row_number().over(w).alias("seq_no"),
        F.ntile(5).over(w2).alias("pressure_bucket"),
    )
)

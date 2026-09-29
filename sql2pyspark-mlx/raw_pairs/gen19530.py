from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_part = Window.partitionBy("device_code").orderBy("status_bits", "id")

result = (
    df_device_readings
    .select(
        "*",
        F.lag("status_bits").over(w_part).alias("prev_status_bits"),
        F.lead("status_bits").over(w_part).alias("next_status_bits"),
        F.lead("pressure").over(w_part).alias("next_pressure"),
        F.lag("pressure").over(w_part).alias("prev_pressure"),
        F.row_number().over(w_part).alias("event_seq"),
    )
)

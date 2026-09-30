from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_running = Window.partitionBy("device_code").orderBy("pressure", "id")

cleaned = (
    df_device_readings
    .filter((F.col("reading_ts") > F.lit("2025-09-01").cast("date")) & F.col("temp_c").between(28, 29))
    .withColumn("pressure_rounded", F.round("pressure", 0))
)

result = (
    cleaned
    .select(
        "*",
        F.lead("status_bits").over(w_running).alias("next_status_bits"),
        F.lag("status_bits").over(w_running).alias("prev_status_bits"),
        F.lead("pressure_rounded").over(w_running).alias("next_pressure_rounded"),
        F.row_number().over(w_running).alias("event_seq"),
    )
)

from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_running = Window.orderBy("status_bits", "id")

cleaned = (
    df_device_readings
    .filter(F.col("status_bits").between(6, 8))
    .select(
        "id",
        "reading_ts",
        "humidity",
        "pressure",
        "status_bits",
        "firmware",
        (F.col("humidity") / F.nullif("status_bits", F.lit(0))).alias("humidity_per_status_bits"),
    )
)

result = (
    cleaned
    .filter(F.col("reading_ts") >= "2024-01-24")
    .select(
        "id",
        "reading_ts",
        "status_bits",
        F.lead("status_bits").over(w_running).alias("next_status_bits"),
        F.lag("pressure").over(w_running).alias("prev_pressure"),
        F.row_number().over(w_running).alias("row_seq"),
    )
)

from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code").orderBy("pressure", "id")

filtered = (
    df_device_readings
    .filter((F.col("reading_ts") >= F.lit("2025-09-01").cast("date")) & (F.col("temp_c") < 24))
    .withColumn("least_pressure_temp_c", F.least("pressure", "temp_c"))
)

result = (
    filtered
    .withColumn("rank_in_group", F.row_number().over(w))
    .filter(F.col("rank_in_group") == 1)
    .select("id", "reading_ts", "humidity", "least_pressure_temp_c")
)

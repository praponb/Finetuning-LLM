from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code")
w2 = Window.orderBy("status_bits", "id").rowsBetween(-5, Window.currentRow)

filtered = (
    df_device_readings
    .filter(F.col("device_code").isin(["DV-01", "DV-03", "DV-04"]))
    .withColumn("pressure_int", F.col("pressure").cast("int"))
)

result = (
    filtered
    .select(
        "id",
        "device_code",
        "reading_ts",
        "temp_c",
        "pressure",
        "status_bits",
        "pressure_int",
        F.max("pressure_int").over(w).alias("device_code_max_pressure_int"),
        F.avg("pressure_int").over(w2).alias("pressure_int_avg_5"),
    )
)

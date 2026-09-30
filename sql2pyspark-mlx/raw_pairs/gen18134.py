from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.year("reading_ts"), "device_code")
w2 = Window.partitionBy(F.year("reading_ts")).orderBy("pressure", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_running = Window.partitionBy("device_code").orderBy(F.col("reading_ts").cast("bigint")).rangeBetween(-7200, Window.currentRow)

base = (
    df_device_readings
    .filter(F.col("reading_ts") >= F.lit("2025-09-01").cast("date"))
    .withColumn("reading_ts_year", F.year("reading_ts"))
)

result = (
    base
    .select(
        "id",
        "temp_c",
        "pressure",
        "status_bits",
        F.corr("pressure", "humidity").over(w).alias("corr_pressure_humidity"),
        F.first("pressure", ignorenulls=True).over(w2).alias("first_known_pressure"),
        F.max("pressure").over(w_running).alias("pressure_max_2h"),
    )
)

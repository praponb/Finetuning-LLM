from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.date_trunc("month", "reading_ts"))
w2 = Window.orderBy("humidity", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.orderBy("pressure", "id")
w4 = Window.orderBy(F.col("temp_c").asc_nulls_first())

base = (
    df_device_readings
    .filter((F.col("reading_ts") >= F.to_timestamp(F.lit("2025-09-01 03:00:00"))) & (F.col("temp_c") < 39))
    .select(
        "id",
        "reading_ts",
        "temp_c",
        "humidity",
        "pressure",
        "status_bits",
        F.when(F.col("pressure") >= 1025, "High").when(F.col("pressure") >= 1002, "Medium").otherwise("Low").alias("pressure_tier"),
    )
)

result = (
    base
    .select(
        "id",
        "reading_ts",
        "temp_c",
        "humidity",
        "pressure",
        "status_bits",
        F.sum("status_bits").over(w).alias("reading_ts_period_sum_status_bits"),
        F.last("pressure", ignorenulls=True).over(w2).alias("last_known_pressure"),
        F.ntile(4).over(w3).alias("pressure_bucket"),
        F.sum("pressure").over(w2).alias("cumulative_pressure"),
        F.rank().over(w4).alias("temp_c_rank"),
    )
)

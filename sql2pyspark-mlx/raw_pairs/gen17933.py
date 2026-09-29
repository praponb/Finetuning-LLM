from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code").orderBy(F.col("reading_ts").cast("bigint")).rangeBetween(-900, Window.currentRow)
w2 = Window.partitionBy("device_code")
w3 = Window.partitionBy("device_code").orderBy("reading_ts", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

result = (
    df_device_readings
    .select(
        "id",
        "reading_ts",
        "pressure",
        "status_bits",
        F.sum("pressure").over(w).alias("pressure_sum_15m"),
        F.round((F.col("humidity") / F.sum("humidity").over(w2)), 4).alias("humidity_share"),
        F.sum("pressure").over(w3).alias("cumulative_pressure"),
        F.corr("temp_c", "pressure").over(w2).alias("corr_temp_c_pressure"),
        F.sum("humidity").over(w3).alias("cumulative_humidity"),
    )
)

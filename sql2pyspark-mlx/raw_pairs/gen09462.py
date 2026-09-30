from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code").orderBy("status_bits")
w2 = Window.partitionBy("device_code").orderBy("temp_c", "id")
w3 = Window.partitionBy("device_code")
w4 = Window.partitionBy("device_code").orderBy("humidity", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.orderBy(F.col("reading_ts").cast("bigint")).rangeBetween(-1800, Window.currentRow)

result = (
    df_device_readings
    .select(
        "id",
        "reading_ts",
        "temp_c",
        "pressure",
        F.dense_rank().over(w).alias("status_bits_dense_rank"),
        F.row_number().over(w2).alias("row_seq"),
        F.corr("pressure", "temp_c").over(w3).alias("corr_pressure_temp_c"),
        F.first("humidity", ignorenulls=True).over(w4).alias("first_known_humidity"),
        F.sum("pressure").over(w5).alias("pressure_sum_30m"),
    )
)

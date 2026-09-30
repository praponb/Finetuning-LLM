from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code").orderBy(F.col("pressure").asc_nulls_last())
w2 = Window.partitionBy("device_code")
w3 = Window.orderBy("is_2_0_1", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

base = (
    df_device_readings
    .filter(F.to_date("reading_ts") < "2023-07-14")
    .select(
        "*",
        F.when(F.col("firmware") == "2.0.1", 1).otherwise(0).alias("is_2_0_1"),
        F.col("status_bits").cast("double").alias("status_bits_str"),
    )
)

result = (
    base
    .withColumn("pressure_percent_rank", F.percent_rank().over(w))
    .withColumn("humidity_share", F.round((F.col("humidity") / F.sum("humidity").over(w2)), 4))
    .withColumn("cumulative_status_bits", F.sum("status_bits").over(w3))
)

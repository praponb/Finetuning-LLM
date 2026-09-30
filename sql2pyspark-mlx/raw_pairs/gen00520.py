from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code").orderBy("temp_c", "id")

deduped = (
    df_device_readings
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") == 1)
    .select("id", "device_code", "reading_ts", "temp_c", "pressure", "firmware")
)

result = (
    deduped
    .groupBy(
        F.when(F.col("pressure") >= 1017, "LARGE").otherwise("SMALL").alias("pressure_tier"),
    )
    .agg(
        F.percentile_approx("pressure", 0.5).alias("pressure_p50"),
        F.stddev_samp("temp_c").alias("temp_c_stddev"),
    )
    .orderBy("pressure_tier")
)

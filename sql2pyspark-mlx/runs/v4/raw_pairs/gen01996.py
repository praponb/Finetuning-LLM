from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code").orderBy(F.col("reading_ts").desc(), "id")

latest = (
    df_device_readings
    .withColumn("dedup_rank", F.row_number().over(w))
    .filter(F.col("dedup_rank") == 1)
)

result = (
    latest
    .select("id", "device_code", "reading_ts", "pressure", "firmware", "dedup_rank")
    .orderBy(F.col("reading_ts").desc_nulls_first(), F.col("pressure").desc())
)

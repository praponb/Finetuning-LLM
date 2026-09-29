from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code").orderBy("humidity", "id")

deduped = (
    df_device_readings
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") == 1)
    .select(
        "id",
        "device_code",
        "reading_ts",
        "temp_c",
        "humidity",
        "pressure",
        "status_bits",
    )
)

result = (
    deduped
    .select("id", "reading_ts", "pressure", "status_bits")
    .orderBy(F.col("pressure").desc(), F.col("id").desc())
)

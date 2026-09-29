from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code").orderBy(F.col("reading_ts").desc(), "id")

deduped = (
    df_device_readings
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("id", "device_code", "temp_c", "humidity", "pressure", "firmware")
)

result = (
    deduped
    .select(
        "id",
        "device_code",
        "temp_c",
        "firmware",
        F.when(F.col("device_code").rlike("^(DV-01|DV-04)$"), "CORE").otherwise("OTHER_ZONE").alias("device_code_zone"),
    )
)

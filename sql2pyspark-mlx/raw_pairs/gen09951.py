from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code").orderBy("status_bits")

deduped = (
    df_device_readings
    .withColumn("rn", F.rank().over(w))
    .filter(F.col("rn") == 1)
)

result = deduped.select("id", "device_code", "humidity", "pressure", "status_bits", "rn")

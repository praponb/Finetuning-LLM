from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code").orderBy(F.col("humidity_minus_temp_c").desc(), "id")

valid = (
    df_device_readings
    .filter(F.col("pressure") > 1014)
    .withColumn("humidity_minus_temp_c", (F.col("humidity") - F.col("temp_c")))
)

result = (
    valid
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("id", "temp_c", "humidity", "pressure", "status_bits", "firmware")
)
